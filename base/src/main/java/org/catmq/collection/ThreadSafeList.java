package org.catmq.collection;

import java.util.concurrent.atomic.AtomicReference;

public class ThreadSafeList<T> {
    private static class Node<T> {
        T value;
        AtomicReference<Node<T>> next;
        int version;

        Node(T value, Node<T> next) {
            this.value = value;
            this.next = new AtomicReference<>(next);
            this.version = 0;
        }
    }

    private AtomicReference<Node<T>> head = new AtomicReference<>(new Node<>(null, null));

    public void add(T value) {
        Node<T> newNode = new Node<>(value, null);
        Node<T> currentNode;
        Node<T> nextNode;

        do {
            currentNode = head.get();
            nextNode = currentNode.next.get();
            newNode.next.set(nextNode);
        } while (!head.compareAndSet(currentNode, new Node<>(null, newNode)));
    }

    public boolean remove(T value) {
        Node<T> currentNode;
        Node<T> nextNode;

        while (true) {
            currentNode = head.get();
            nextNode = currentNode.next.get();

            if (nextNode == null) {
                return false;
            } else if (nextNode.value.equals(value)) {
                if (currentNode.next.compareAndSet(nextNode, nextNode.next.get())) {
                    return true;
                }
            }
        }
    }
}
