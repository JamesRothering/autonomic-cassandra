/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.autonomic;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.detectors.RepairOverdueDetector;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.AdvertisedSeverityActuator;
import org.apache.cassandra.autonomic.execute.actuators.CacheCapacityActuator;
import org.apache.cassandra.autonomic.execute.actuators.ConcurrentCompactorsActuator;
import org.apache.cassandra.autonomic.execute.actuators.HintThrottleActuator;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator;
import org.apache.cassandra.autonomic.execute.actuators.ScaleOutActuator;
import org.apache.cassandra.autonomic.execute.actuators.StreamThroughputActuator;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.policies.LoadSpikePolicy;
import org.apache.cassandra.autonomic.plan.policies.RequestPoolPolicy;
import org.apache.cassandra.autonomic.plan.policies.RequestThrottlePolicy;
import org.apache.cassandra.autonomic.plan.policies.Windows;
import org.apache.cassandra.autonomic.plan.policies.YieldBackgroundPolicy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Postmortem 2026-11-22, R2: every input the layer acts on has a producer, and every way an actuator can move is
 * proposed by some policy, unless recorded below with a reason and the item of
 * {@code docs/audits/observability-coverage.md} that tracks it.
 *
 * <ul>
 * <li>Producers are the sensors a started {@link AutonomicService} registers (test-only sensors are not; the test
 * registers none and checks each sensor class comes from the main tree). Each declares its output in
 * {@link Sensor#signals}.</li>
 * <li>Consumers declare their inputs: {@link Detector#requires} and {@link Policy#inputs}. Each declaration is
 * checked against the bytecode: the {@link SignalId} constants the class reads (its own methods, its nested and
 * anonymous classes, the methods it inherits, and what those call statically or on {@code this}; a static method
 * counts only when called) must all be
 * declared. A sensor's reads must likewise be in its {@link Sensor#signals}.</li>
 * <li>As a backstop, every {@link SignalId} constant read anywhere in the layer outside the sensors (verify specs and
 * guards built by actuators or the service included) must have a producer.</li>
 * <li>Directions: a knob actuator ({@link KnobActuator}) can move {@code UP} and {@code DOWN}, any other actuator has
 * one, {@link Policy#APPLY}. Every actuator is counted, because each can be enabled in {@code cassandra.yaml}.
 * Policies declare what they propose in {@link Policy#actuations}; the declaration must name a real actuator and
 * direction, and every actuator name the policy's code mentions must be declared. The direction of a non-knob
 * policy's move is declared by hand and is not machine-checked.</li>
 * </ul>
 */
public class InputCoverageTest extends CQLTester
{
    private static final String AUDIT = "docs/audits/observability-coverage.md";
    private static final String ROOT = "org/apache/cassandra/autonomic/";
    private static final String SENSORS_PACKAGE = ROOT + "monitor/sensors/";
    private static final String SIGNAL_ID = "org/apache/cassandra/autonomic/monitor/SignalId";
    private static final String SIGNAL_ID_DESC = 'L' + SIGNAL_ID + ';';

    /** Signals with no sensor: derived inside the layer. Each needs a reason. */
    private static final Map<SignalId, String> DERIVED = Map.of(
        RepairOverdueDetector.OVERDUE_RATIO,
        "computed by RepairOverdueDetector from auto-repair history, never stored; the nominal goal of a repair_priority action, "
        + "a cluster action whose actuator verifies itself (" + AUDIT + " RC-6)");

    /** Classes other than Signals that may make a SignalId from a string. */
    private static final Map<String, String> SIGNAL_ID_FACTORIES = Map.of(
        "org.apache.cassandra.autonomic.analyze.detectors.RepairOverdueDetector", "defines the derived signal in DERIVED",
        ScaleOutActuator.class.getName(), "re-reads the goal it wrote to its claim; the goal comes from ScaleOutPolicy, whose inputs are checked");

    /** Actuator directions no policy proposes, each with a reason. */
    private static final Map<String, String> UNUSED_DIRECTIONS = Map.of(
        Policy.actuation(CacheCapacityActuator.KEY, "UP"), "planned in overview 8.1 (up on low hit rate with heap headroom), never wired: no hit-rate or headroom input (" + AUDIT + " RC-1, T7.15/T7.16)",
        Policy.actuation(CacheCapacityActuator.ROW, "UP"), "as key_cache_capacity UP (" + AUDIT + " RC-1)",
        Policy.actuation(CacheCapacityActuator.COUNTER, "UP"), "as key_cache_capacity UP (" + AUDIT + " RC-1)",
        Policy.actuation(ConcurrentCompactorsActuator.NAME, "DOWN"), "planned in overview 8.1 (down under foreground pressure), never wired (" + AUDIT + " RC-2)",
        Policy.actuation(StreamThroughputActuator.NAME, "UP"), "8.1 plans only down; a yielded value stays until an operator revert (" + AUDIT + " RC-3)",
        Policy.actuation(HintThrottleActuator.NAME, "UP"), "8.1 plans only down; a yielded value stays until an operator revert (" + AUDIT + " RC-3)",
        Policy.actuation(AdvertisedSeverityActuator.NAME, "DOWN"), "8.1 plans only up; a raised severity stays until an operator revert (" + AUDIT + " RC-4)");

    /** Actuators a policy's code names without proposing them, as "policy/actuator", each with a reason. */
    private static final Map<String, String> NAMED_NOT_PROPOSED = Map.of(
        "partition_unsplit/partition_split", "only looks for a split in flight on the key before it proposes an unsplit");

    private AutonomicService service;
    private final Map<String, ClassData> classes = new HashMap<>();

    @Before
    public void startLayer()
    {
        DatabaseDescriptor.getAutonomicConfig().enabled = true;
        service = new AutonomicService(new AtomicLong(1_000_000)::get, () -> true);
        service.start();
    }

    @After
    public void stopLayer() throws Exception
    {
        if (service != null)
            service.shutdownBlocking();
        DatabaseDescriptor.getAutonomicConfig().enabled = false;
    }

    @Test
    public void sensorsComeFromTheMainTreeAndEmitOnlyWhatTheyDeclare() throws Exception
    {
        Object main = Signals.class.getProtectionDomain().getCodeSource().getLocation();
        assertTrue(service.sensors().size() >= 10);
        for (Sensor sensor : service.sensors())
        {
            assertEquals(sensor.name() + " is not a production sensor", main, sensor.getClass().getProtectionDomain().getCodeSource().getLocation());
            assertTrue(sensor.name() + " declares signals Signals does not define",
                       Signals.all().containsAll(sensor.signals()));
            assertSubset(sensor.getClass().getName() + " emits signals it does not declare in signals()", reads(sensor.getClass()), sensor.signals());
        }
        assertSubset("signals defined in Signals that no registered sensor emits", Signals.all(), produced());
    }

    @Test
    public void detectorInputsAreProducedAndDeclared() throws Exception
    {
        Set<SignalId> produced = produced();
        assertTrue(service.builtinDetectors().size() >= 10);
        for (Detector detector : service.builtinDetectors())
        {
            assertSubset("detector " + detector.name() + " requires signals no sensor emits", detector.requires(), produced);
            assertSubset("detector " + detector.name() + " (" + detector.getClass().getName() + ") reads signals it does not declare in requires()",
                         reads(detector.getClass()), union(detector.requires(), DERIVED.keySet()));
        }
    }

    @Test
    public void policyInputsAreProducedAndDeclared() throws Exception
    {
        Set<SignalId> producedOrDerived = union(produced(), DERIVED.keySet());
        for (Policy policy : policiesAndLadderParts())
        {
            assertSubset("policy " + policy.name() + " needs signals no sensor emits", policy.inputs(), producedOrDerived);
            assertSubset("policy " + policy.name() + " (" + policy.getClass().getName() + ") reads signals it does not declare in inputs()",
                         reads(policy.getClass()), policy.inputs());
        }
        LoadSpikePolicy ladder = (LoadSpikePolicy) policy(LoadSpikePolicy.NAME);
        Set<SignalId> parts = new HashSet<>();
        Set<String> moves = new HashSet<>();
        for (Policy part : ladderParts())
        {
            parts.addAll(part.inputs());
            moves.addAll(part.actuations());
        }
        parts.addAll(policy("scale_out").inputs());
        moves.addAll(policy("scale_out").actuations());
        assertEquals("the ladder declares the inputs of its steps", parts, ladder.inputs());
        assertEquals("the ladder declares the moves of its steps", moves, ladder.actuations());
    }

    @Test
    public void everySignalReadInTheLayerIsProduced() throws Exception
    {
        Set<SignalId> producedOrDerived = union(produced(), DERIVED.keySet());
        List<String> problems = new ArrayList<>();
        List<String> all = allLayerClasses();
        assertTrue(all.size() > 100);
        for (String name : all)
        {
            ClassData c = load(name);
            boolean sensor = name.startsWith(SENSORS_PACKAGE);
            Set<SignalId> read = new HashSet<>();
            for (MethodData m : c.methods.values())
            {
                if (!sensor)
                    for (String[] f : m.fields)
                        read.addAll(value(f[0], f[1]));
                for (Call call : m.calls)
                    if (call.owner.equals(SIGNAL_ID) && call.name.equals("of") && !name.equals(SENSORS_PACKAGE + "Signals")
                        && !SIGNAL_ID_FACTORIES.containsKey(name.replace('/', '.')))
                        problems.add(name + " makes a SignalId from a string; declare it in Signals");
            }
            for (SignalId s : read)
                if (!producedOrDerived.contains(s))
                    problems.add(name + " reads " + s.name + ", which no sensor emits");
        }
        assertTrue(String.join("\n", problems), problems.isEmpty());
        for (String factory : SIGNAL_ID_FACTORIES.keySet())
            assertTrue("stale entry " + factory, all.contains(factory.replace('.', '/')));
    }

    @Test
    public void everyActuatorDirectionIsProposedOrRecorded() throws Exception
    {
        Map<String, Set<String>> directions = new HashMap<>();
        for (Actuator a : service.actuators())
            directions.put(a.name(), a instanceof KnobActuator ? Set.of("UP", "DOWN") : Set.of(Policy.APPLY));
        assertTrue(directions.size() >= 15);

        Set<String> proposed = new HashSet<>();
        for (Policy policy : policiesAndLadderParts())
        {
            Set<String> declaredActuators = new HashSet<>();
            for (String move : policy.actuations())
            {
                int slash = move.indexOf('/');
                String actuator = move.substring(0, slash);
                assertTrue("policy " + policy.name() + " declares " + move + " of an actuator that is not registered", directions.containsKey(actuator));
                assertTrue("policy " + policy.name() + " declares " + move + ", not a direction of " + actuator,
                           directions.get(actuator).contains(move.substring(slash + 1)));
                declaredActuators.add(actuator);
            }
            Set<String> named = new TreeSet<>(strings(policy.getClass()));
            named.retainAll(directions.keySet());
            named.removeIf(actuator -> NAMED_NOT_PROPOSED.containsKey(policy.name() + '/' + actuator));
            assertSubset("policy " + policy.name() + " names actuators it does not declare in actuations()", named, declaredActuators);
        }
        for (Policy policy : service.builtinPolicies())
            proposed.addAll(policy.actuations());

        Set<String> unused = new TreeSet<>();
        for (Map.Entry<String, Set<String>> e : directions.entrySet())
            for (String d : e.getValue())
                if (!proposed.contains(Policy.actuation(e.getKey(), d)))
                    unused.add(Policy.actuation(e.getKey(), d));
        Set<String> unexplained = new TreeSet<>(unused);
        unexplained.removeAll(UNUSED_DIRECTIONS.keySet());
        assertTrue("actuator directions no policy proposes; wire them or record them in UNUSED_DIRECTIONS with a reason and an audit item: " + unexplained,
                   unexplained.isEmpty());
        Set<String> stale = new TreeSet<>(UNUSED_DIRECTIONS.keySet());
        stale.removeAll(unused);
        assertTrue("UNUSED_DIRECTIONS entries that are now proposed or no longer exist; remove them: " + stale, stale.isEmpty());
    }

    // ---- components ----

    private Set<SignalId> produced()
    {
        Set<SignalId> out = new HashSet<>();
        for (Sensor s : service.sensors())
            out.addAll(s.signals());
        return out;
    }

    private Policy policy(String name)
    {
        for (Policy p : service.builtinPolicies())
            if (p.name().equals(name))
                return p;
        throw new AssertionError("no policy " + name);
    }

    /** The steps of the load spike ladder wrap policies that are not registered on their own. */
    private List<Policy> ladderParts()
    {
        java.util.function.Function<String, Actuator> actuators = name -> {
            for (Actuator a : service.actuators())
                if (a.name().equals(name))
                    return a;
            return null;
        };
        java.util.function.Function<String, Windows> windows = name -> new Windows(Windows.DEFAULT_BASELINE_MILLIS, Windows.DEFAULT_OBSERVATION_MILLIS);
        return List.of(YieldBackgroundPolicy.background(actuators, windows, name -> true),
                       YieldBackgroundPolicy.advertise(actuators, windows, name -> true),
                       new RequestPoolPolicy(actuators, windows, RequestPoolPolicy.Mode.SATURATION),
                       new RequestThrottlePolicy(actuators, windows, RequestThrottlePolicy.Mode.BREACH));
    }

    private List<Policy> policiesAndLadderParts()
    {
        List<Policy> out = new ArrayList<>(service.builtinPolicies());
        assertTrue(out.size() >= 10);
        out.addAll(ladderParts());
        return out;
    }

    private static void assertSubset(String message, Set<?> subset, Set<?> superset)
    {
        Set<String> missing = new TreeSet<>();
        for (Object o : subset)
            if (!superset.contains(o))
                missing.add(o instanceof SignalId ? ((SignalId) o).name : String.valueOf(o));
        if (!missing.isEmpty())
            fail(message + ": " + missing);
    }

    private static <T> Set<T> union(Set<T> a, Set<T> b)
    {
        Set<T> out = new HashSet<>(a);
        out.addAll(b);
        return out;
    }

    // ---- bytecode ----

    private static final class Call
    {
        final int opcode;
        final String owner;
        final String name;
        final String desc;

        Call(int opcode, String owner, String name, String desc)
        {
            this.opcode = opcode;
            this.owner = owner;
            this.name = name;
            this.desc = desc;
        }
    }

    private static final class MethodData
    {
        final String owner;
        final String key;
        final int access;
        final List<String[]> fields = new ArrayList<>();
        final Set<String> strings = new HashSet<>();
        final List<Call> calls = new ArrayList<>();

        MethodData(String owner, String key, int access)
        {
            this.owner = owner;
            this.key = key;
            this.access = access;
        }
    }

    private static final class ClassData
    {
        String superName;
        final Set<String> inner = new LinkedHashSet<>();
        final Map<String, MethodData> methods = new HashMap<>();
    }

    private ClassData load(String internalName) throws IOException
    {
        ClassData cached = classes.get(internalName);
        if (cached != null)
            return cached;
        ClassData c = new ClassData();
        try (InputStream in = getClass().getClassLoader().getResourceAsStream(internalName + ".class"))
        {
            if (in == null)
                throw new IOException("no class file for " + internalName);
            new ClassReader(in).accept(new ClassVisitor(Opcodes.ASM9)
            {
                @Override
                public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
                {
                    c.superName = superName;
                }

                @Override
                public void visitInnerClass(String name, String outerName, String innerName, int access)
                {
                    if (name.startsWith(internalName + '$'))
                        c.inner.add(name);
                }

                @Override
                public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
                {
                    MethodData m = new MethodData(internalName, name + desc, access);
                    c.methods.put(m.key, m);
                    return new MethodVisitor(Opcodes.ASM9)
                    {
                        @Override
                        public void visitFieldInsn(int opcode, String owner, String fname, String fdesc)
                        {
                            if (opcode == Opcodes.GETSTATIC && fdesc.endsWith(SIGNAL_ID_DESC))
                                m.fields.add(new String[]{ owner, fname });
                        }

                        @Override
                        public void visitLdcInsn(Object value)
                        {
                            if (value instanceof String)
                                m.strings.add((String) value);
                        }

                        @Override
                        public void visitMethodInsn(int opcode, String owner, String mname, String mdesc, boolean isInterface)
                        {
                            m.calls.add(new Call(opcode, owner, mname, mdesc));
                        }

                        @Override
                        public void visitInvokeDynamicInsn(String iname, String idesc, Handle bsm, Object... args)
                        {
                            for (Object arg : args)
                            {
                                if (!(arg instanceof Handle))
                                    continue;
                                Handle h = (Handle) arg;
                                int opcode = h.getTag() == Opcodes.H_INVOKESTATIC ? Opcodes.INVOKESTATIC
                                           : h.getTag() == Opcodes.H_INVOKESPECIAL || h.getTag() == Opcodes.H_NEWINVOKESPECIAL ? Opcodes.INVOKESPECIAL
                                           : Opcodes.INVOKEVIRTUAL;
                                m.calls.add(new Call(opcode, h.getOwner(), h.getName(), h.getDesc()));
                            }
                        }
                    };
                }
            }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        }
        classes.put(internalName, c);
        return c;
    }

    /** The class and its superclasses inside the layer, most specific first. */
    private List<String> chain(String internalName) throws IOException
    {
        List<String> out = new ArrayList<>();
        for (String n = internalName; n != null && n.startsWith(ROOT); n = load(n).superName)
            out.add(n);
        return out;
    }

    /** The method a call resolves to, searching the chain from {@code start} up; null outside the layer or if abstract. */
    private MethodData resolve(String start, String key) throws IOException
    {
        for (String n : chain(start))
        {
            MethodData m = load(n).methods.get(key);
            if (m != null)
                return (m.access & Opcodes.ACC_ABSTRACT) != 0 ? null : m;
        }
        return null;
    }

    /** The methods that run on behalf of an instance of the class, see the class comment. */
    private List<MethodData> reachable(Class<?> type) throws IOException
    {
        String root = type.getName().replace('.', '/');
        List<String> chain = chain(root);
        Deque<MethodData> work = new ArrayDeque<>();
        Deque<String> own = new ArrayDeque<>(List.of(root));
        Set<String> seenClasses = new HashSet<>();
        while (!own.isEmpty())
        {
            String n = own.pop();
            if (!seenClasses.add(n))
                continue;
            ClassData c = load(n);
            // static methods (factories of other instances, for example) count only when something calls them
            for (MethodData m : c.methods.values())
                if ((m.access & Opcodes.ACC_STATIC) == 0 || m.key.startsWith("<clinit>"))
                    work.add(m);
            own.addAll(c.inner);
        }
        for (String s : chain.subList(1, chain.size()))
            for (MethodData m : load(s).methods.values())
                if (m.key.startsWith("<init>") || ((m.access & Opcodes.ACC_STATIC) == 0 && resolve(root, m.key) == m))
                    work.add(m);

        List<MethodData> out = new ArrayList<>();
        Set<MethodData> seen = new HashSet<>();
        while (!work.isEmpty())
        {
            MethodData m = work.pop();
            if (!seen.add(m))
                continue;
            out.add(m);
            for (Call call : m.calls)
            {
                if (!call.owner.startsWith(ROOT))
                    continue;
                String key = call.name + call.desc;
                MethodData target = null;
                if (call.opcode == Opcodes.INVOKESTATIC || (call.opcode == Opcodes.INVOKESPECIAL && (call.name.equals("<init>") || chain.contains(call.owner))))
                    target = resolve(call.owner, key);
                else if (chain.contains(call.owner))
                    target = resolve(root, key);
                if (target != null)
                    work.add(target);
            }
        }
        return out;
    }

    private Set<SignalId> reads(Class<?> type) throws Exception
    {
        Set<SignalId> out = new HashSet<>();
        for (MethodData m : reachable(type))
            for (String[] f : m.fields)
                out.addAll(value(f[0], f[1]));
        return out;
    }

    private Set<String> strings(Class<?> type) throws Exception
    {
        Set<String> out = new HashSet<>();
        for (MethodData m : reachable(type))
            out.addAll(m.strings);
        return out;
    }

    /** The SignalId (or SignalId array elements) held by a static field. */
    private static Set<SignalId> value(String owner, String name)
    {
        try
        {
            Field f = Class.forName(owner.replace('/', '.'), true, InputCoverageTest.class.getClassLoader()).getDeclaredField(name);
            f.setAccessible(true);
            Object v = f.get(null);
            Set<SignalId> out = new HashSet<>();
            if (v instanceof SignalId)
                out.add((SignalId) v);
            else if (v != null && v.getClass().isArray())
                for (int i = 0; i < Array.getLength(v); i++)
                    out.add((SignalId) Array.get(v, i));
            return out;
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("cannot read " + owner + '.' + name, e);
        }
    }

    private static List<String> allLayerClasses() throws Exception
    {
        Path base = Paths.get(Signals.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        if (Files.isRegularFile(base))
        {
            try (JarFile jar = new JarFile(base.toFile()))
            {
                return jar.stream().map(JarEntry::getName)
                          .filter(n -> n.startsWith(ROOT) && n.endsWith(".class"))
                          .map(n -> n.substring(0, n.length() - ".class".length()))
                          .sorted()
                          .collect(Collectors.toList());
            }
        }
        Path dir = base.resolve(ROOT);
        try (Stream<Path> files = Files.walk(dir))
        {
            return files.filter(p -> p.toString().endsWith(".class"))
                        .map(p -> base.relativize(p).toString().replace('\\', '/'))
                        .map(s -> s.substring(0, s.length() - ".class".length()))
                        .sorted()
                        .collect(Collectors.toList());
        }
    }
}
