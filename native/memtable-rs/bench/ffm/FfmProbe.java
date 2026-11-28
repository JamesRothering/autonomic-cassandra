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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

/**
 * JDK 25 only, standalone (no Cassandra): the per-call cost of calling the same Rust library through the FFM API
 * (java.lang.foreign) versus JNI. Both call a function that returns its argument, and one that copies a 160-byte
 * array into Rust and sums it (the size of a benchmark update record).
 *
 *   javac -d /tmp/ffm native/memtable-rs/bench/ffm/FfmProbe.java
 *   java --enable-native-access=ALL-UNNAMED -cp /tmp/ffm FfmProbe native/memtable-rs/target/release/libmemtable_jni.so
 *
 * The library must be built with the `ffm-probe` feature (it adds the two C-ABI functions).
 */
public final class FfmProbe
{
    static native long nopRaw(long x);         // bound to Java_FfmProbe_nopRaw by JNI name lookup
    static native long sumRaw(byte[] b, int n); // bound to Java_FfmProbe_sumRaw

    public static void main(String[] args) throws Throwable
    {
        Path lib = Path.of(args[0]).toAbsolutePath();
        System.load(lib.toString());
        Linker linker = Linker.nativeLinker();
        SymbolLookup symbols = SymbolLookup.libraryLookup(lib, Arena.global());
        MethodHandle nop = linker.downcallHandle(symbols.find("memtable_ffm_nop").orElseThrow(),
                                                 FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
        MethodHandle nopCritical = linker.downcallHandle(symbols.find("memtable_ffm_nop").orElseThrow(),
                                                         FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG),
                                                         Linker.Option.critical(false));
        MethodHandle sum = linker.downcallHandle(symbols.find("memtable_ffm_sum").orElseThrow(),
                                                 FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
        MethodHandle sumCritical = linker.downcallHandle(symbols.find("memtable_ffm_sum").orElseThrow(),
                                                         FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG),
                                                         Linker.Option.critical(true));
        byte[] record = new byte[160];
        for (int i = 0; i < record.length; i++)
            record[i] = (byte) i;
        int n = 20_000_000;
        long sink = 0;
        try (Arena arena = Arena.ofConfined())
        {
            MemorySegment offHeap = arena.allocate(record.length);
            for (int round = 0; round < 6; round++)
            {
                boolean report = round >= 2;
                long t0 = System.nanoTime();
                for (int i = 0; i < n; i++)
                    sink += nopRaw(i);
                print(report, "JNI  nop (empty body)", t0, n);
                t0 = System.nanoTime();
                for (int i = 0; i < n; i++)
                    sink += (long) nop.invokeExact((long) i);
                print(report, "FFM  nop", t0, n);
                t0 = System.nanoTime();
                for (int i = 0; i < n; i++)
                    sink += (long) nopCritical.invokeExact((long) i);
                print(report, "FFM  nop, critical", t0, n);
                int m = n / 10;
                t0 = System.nanoTime();
                for (int i = 0; i < m; i++)
                    sink += sumRaw(record, record.length);
                print(report, "JNI  copy 160 B in (GetByteArrayRegion) and sum", t0, m);
                t0 = System.nanoTime();
                for (int i = 0; i < m; i++)
                {
                    MemorySegment.copy(record, 0, offHeap, ValueLayout.JAVA_BYTE, 0, record.length);
                    sink += (long) sum.invokeExact(offHeap, (long) record.length);
                }
                print(report, "FFM  copy 160 B to native memory and sum", t0, m);
                t0 = System.nanoTime();
                for (int i = 0; i < m; i++)
                    sink += (long) sumCritical.invokeExact(MemorySegment.ofArray(record), (long) record.length);
                print(report, "FFM  critical, heap array passed directly, sum", t0, m);
            }
        }
        System.out.println("sink " + sink);
    }

    private static void print(boolean report, String what, long t0, long ops)
    {
        if (report)
            System.out.println(String.format("ffm-probe: %-50s %6.1f ns/call", what, (System.nanoTime() - t0) / (double) ops));
    }
}
