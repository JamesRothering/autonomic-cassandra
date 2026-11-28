// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An ordered index of `u32` item ids whose nodes live in a [`Pool`].
//!
//! The tree does not know what an item is: every operation takes a function
//! that compares the probe with an item (by looking the item up in the
//! caller's storage). That is what lets the keys stay in a [`crate::region`]
//! instead of in the tree, and what makes the tree's own memory a handful of
//! pool chunks.
//!
//! It is the textbook B-tree of minimum degree [`T`] (Cormen et al., chapter
//! 18): items in every node, insertion top-down with full nodes split on the
//! way, so no step ever walks back up. There is no removal: a memtable only
//! grows until it is discarded.

use std::cmp::Ordering;
use std::ops::ControlFlow;

use crate::region::{Full, NIL, Pool};

/// Minimum degree: every node but the root holds `T - 1 ..= 2T - 1` items.
const T: usize = 8;
const MAX_ITEMS: usize = 2 * T - 1;

/// 128 bytes: a length, a leaf flag, 15 items and 16 children.
#[derive(Clone, Copy, Debug)]
pub struct Node {
    len: u8,
    leaf: bool,
    items: [u32; MAX_ITEMS],
    children: [u32; MAX_ITEMS + 1],
}

impl Node {
    fn leaf() -> Self {
        Self {
            len: 0,
            leaf: true,
            items: [NIL; MAX_ITEMS],
            children: [NIL; MAX_ITEMS + 1],
        }
    }

    fn items(&self) -> &[u32] {
        &self.items[..usize::from(self.len)]
    }

    /// Where the probe is (`Ok`) or would go (`Err`), given `cmp(item)` =
    /// `probe.cmp(item)`.
    fn search(&self, cmp: &mut impl FnMut(u32) -> Ordering) -> Result<usize, usize> {
        self.items().binary_search_by(|&item| cmp(item).reverse())
    }
}

/// A tree's root; the nodes are in the pool passed to each operation.
#[derive(Clone, Copy, Debug)]
pub struct Tree {
    root: u32,
}

impl Default for Tree {
    fn default() -> Self {
        Self { root: NIL }
    }
}

impl Tree {
    /// The item equal to the probe, given `cmp(item)` = `probe.cmp(item)`.
    pub fn find(self, nodes: &Pool<Node>, mut cmp: impl FnMut(u32) -> Ordering) -> Option<u32> {
        let mut x = self.root;
        while x != NIL {
            let node = nodes.get(x);
            match node.search(&mut cmp) {
                Ok(i) => return Some(node.items[i]),
                Err(_) if node.leaf => return None,
                Err(i) => x = node.children[i],
            }
        }
        None
    }

    /// Inserts `item`, which must compare as the probe of `cmp` and must not
    /// be in the tree yet.
    ///
    /// On error the tree is unchanged except for splits, which keep it a
    /// valid tree of the same items.
    ///
    /// # Errors
    /// [`Full`] if the pool cannot take another node.
    ///
    /// # Panics
    /// If an item equal to the probe is already in the tree: the caller
    /// broke the precondition (it must [`Tree::find`] first, under the same
    /// lock). A panic poisons the shard; silently keeping the new item out of
    /// the tree would lose data.
    pub fn insert(
        &mut self,
        nodes: &mut Pool<Node>,
        item: u32,
        mut cmp: impl FnMut(u32) -> Ordering,
    ) -> Result<(), Full> {
        if self.root == NIL {
            let mut node = Node::leaf();
            node.items[0] = item;
            node.len = 1;
            self.root = nodes.push(node)?;
            return Ok(());
        }
        if usize::from(nodes.get(self.root).len) == MAX_ITEMS {
            let mut new_root = Node::leaf();
            new_root.leaf = false;
            new_root.children[0] = self.root;
            let s = nodes.push(new_root)?;
            split_child(nodes, s, 0)?;
            self.root = s;
        }
        let mut x = self.root;
        loop {
            let node = *nodes.get(x);
            let Err(mut i) = node.search(&mut cmp) else {
                panic!("B-tree insert of an item that is already present");
            };
            if node.leaf {
                let n = nodes.get_mut(x);
                let len = usize::from(n.len);
                n.items.copy_within(i..len, i + 1);
                n.items[i] = item;
                n.len += 1;
                return Ok(());
            }
            let child = node.children[i];
            if usize::from(nodes.get(child).len) == MAX_ITEMS {
                split_child(nodes, x, i)?;
                // The child's median moved up to position i.
                if cmp(nodes.get(x).items[i]) == Ordering::Greater {
                    i += 1;
                }
            }
            x = nodes.get(x).children[i];
        }
    }

    /// Calls `f` on the items in order, starting at the first item for which
    /// `is_before` is false, until `f` breaks. `is_before` must be true for a
    /// prefix of the items in order and false for the rest.
    pub fn for_each_from<B>(
        self,
        nodes: &Pool<Node>,
        mut is_before: impl FnMut(u32) -> bool,
        mut f: impl FnMut(u32) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if self.root == NIL {
            return ControlFlow::Continue(());
        }
        walk(nodes, self.root, &mut is_before, &mut f)
    }

    /// Calls `f` on every item in order.
    pub fn for_each<B>(
        self,
        nodes: &Pool<Node>,
        f: impl FnMut(u32) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.for_each_from(nodes, |_| false, f)
    }
}

/// In-order walk of the subtree at `x` from the first item not before the
/// bound. The recursion is as deep as the tree (at most about 11 levels for
/// 2^32 items).
fn walk<B>(
    nodes: &Pool<Node>,
    x: u32,
    is_before: &mut impl FnMut(u32) -> bool,
    f: &mut impl FnMut(u32) -> ControlFlow<B>,
) -> ControlFlow<B> {
    let node = nodes.get(x);
    let first = node.items().partition_point(|&item| is_before(item));
    if !node.leaf {
        walk(nodes, node.children[first], is_before, f)?;
    }
    for j in first..usize::from(node.len) {
        f(node.items[j])?;
        if !node.leaf {
            // Every item in this subtree is after the bound, so `is_before`
            // only costs a few comparisons per node here.
            walk(nodes, node.children[j + 1], is_before, f)?;
        }
    }
    ControlFlow::Continue(())
}

/// Splits the full child `i` of `x` around its median, which moves up into
/// `x` (not full). The new node is allocated before anything is modified.
fn split_child(nodes: &mut Pool<Node>, x: u32, i: usize) -> Result<(), Full> {
    let y_index = nodes.get(x).children[i];
    let y = *nodes.get(y_index);
    let mut z = Node::leaf();
    z.leaf = y.leaf;
    z.items[..T - 1].copy_from_slice(&y.items[T..MAX_ITEMS]);
    if !y.leaf {
        z.children[..T].copy_from_slice(&y.children[T..=MAX_ITEMS]);
    }
    #[allow(clippy::cast_possible_truncation)] // T - 1 = 7
    {
        z.len = (T - 1) as u8;
    }
    let z_index = nodes.push(z)?;

    let median = y.items[T - 1];
    let y_node = nodes.get_mut(y_index);
    #[allow(clippy::cast_possible_truncation)] // T - 1 = 7
    {
        y_node.len = (T - 1) as u8;
    }
    // Clear the moved slots so a stale id can never be read as live.
    y_node.items[T - 1..].fill(NIL);
    if !y_node.leaf {
        y_node.children[T..].fill(NIL);
    }

    let x_node = nodes.get_mut(x);
    let len = usize::from(x_node.len);
    x_node.children.copy_within(i + 1..=len, i + 2);
    x_node.children[i + 1] = z_index;
    x_node.items.copy_within(i..len, i + 1);
    x_node.items[i] = median;
    x_node.len += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Items are their own keys here: ids are values.
    fn insert_all(values: &[u32]) -> (Tree, Pool<Node>, BTreeSet<u32>) {
        let mut tree = Tree::default();
        let mut nodes = Pool::default();
        let mut model = BTreeSet::new();
        for &v in values {
            if tree.find(&nodes, |item| v.cmp(&item)).is_none() {
                tree.insert(&mut nodes, v, |item| v.cmp(&item)).unwrap();
            }
            model.insert(v);
        }
        (tree, nodes, model)
    }

    fn collect_from(tree: Tree, nodes: &Pool<Node>, lower: u32) -> Vec<u32> {
        let mut out = Vec::new();
        let _ = tree.for_each_from::<()>(
            nodes,
            |item| item < lower,
            |item| {
                out.push(item);
                ControlFlow::Continue(())
            },
        );
        out
    }

    #[test]
    fn matches_a_btreeset() {
        let n = if cfg!(miri) { 300 } else { 20_000 };
        // A scrambled sequence with repeats.
        let values: Vec<u32> = (0..n).map(|i| (i * 7919 + 13) % (n / 2 * 3)).collect();
        let (tree, nodes, model) = insert_all(&values);
        for v in 0..n * 2 {
            assert_eq!(
                tree.find(&nodes, |item| v.cmp(&item)).is_some(),
                model.contains(&v)
            );
        }
        for lower in [0, 1, 17, n / 3, n, n * 2] {
            let expected: Vec<u32> = model.range(lower..).copied().collect();
            assert_eq!(collect_from(tree, &nodes, lower), expected);
        }
    }

    #[test]
    fn walk_stops_when_told() {
        let (tree, nodes, _) = insert_all(&(0..1000).rev().collect::<Vec<_>>());
        let mut seen = Vec::new();
        let r = tree.for_each_from(
            &nodes,
            |item| item < 500,
            |item| {
                if item == 510 {
                    return ControlFlow::Break(item);
                }
                seen.push(item);
                ControlFlow::Continue(())
            },
        );
        assert_eq!(r, ControlFlow::Break(510));
        assert_eq!(seen, (500..510).collect::<Vec<_>>());
    }

    #[test]
    fn empty_tree() {
        let tree = Tree::default();
        let nodes = Pool::default();
        assert!(tree.find(&nodes, |_| Ordering::Equal).is_none());
        assert_eq!(collect_from(tree, &nodes, 0), Vec::<u32>::new());
    }
}
