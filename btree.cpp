#include <iostream>
#include <string>
#include <vector>
using namespace std;

template <typename KeyType, typename ValueType>
class BTreeNode {
    KeyType *keys;
    int t;
    BTreeNode **C;
    int n;
    bool leaf;
    ValueType *values;
    template <typename, typename>
    friend class BTree;

   public:
    BTreeNode(int _t, bool _leaf);

    void insertNonFull(KeyType k, ValueType v);
    void splitChild(int i, BTreeNode *y);
    void traverse();
    void traverse(std::vector<ValueType> &result);

    BTreeNode *search_node(KeyType k);
    ValueType search(KeyType k);
};

template <typename KeyType, typename ValueType>
void BTreeNode<KeyType, ValueType>::insertNonFull(KeyType k, ValueType v) {
    int i = n - 1;

    if (leaf == true) {
        while (i >= 0 && keys[i] > k) {
            keys[i + 1] = keys[i];
            values[i + 1] = values[i];
            i--;
        }

        keys[i + 1] = k;
        values[i + 1] = v;
        n = n + 1;
    } else {
        while (i >= 0 && keys[i] > k)
            i--;

        if (C[i + 1]->n == 2 * t - 1) {
            splitChild(i + 1, C[i + 1]);

            if (keys[i + 1] < k)
                i++;
        }
        C[i + 1]->insertNonFull(k, v);
    }
}

template <typename KeyType, typename ValueType>
void BTreeNode<KeyType, ValueType>::splitChild(int i, BTreeNode *y) {
    BTreeNode *z = new BTreeNode(y->t, y->leaf);
    z->n = t - 1;

    for (int j = 0; j < t - 1; j++) {
        z->keys[j] = y->keys[j + t];
        z->values[j] = y->values[j + t];
    }

    if (y->leaf == false) {
        for (int j = 0; j < t; j++)
            z->C[j] = y->C[j + t];
    }

    y->n = t - 1;
    for (int j = n; j >= i + 1; j--)
        C[j + 1] = C[j];

    C[i + 1] = z;

    for (int j = n - 1; j >= i; j--) {
        keys[j + 1] = keys[j];
        values[j + 1] = values[j];
    }

    keys[i] = y->keys[t - 1];
    values[i] = y->values[t - 1];
    n = n + 1;
}

template <typename KeyType, typename ValueType>
BTreeNode<KeyType, ValueType>::BTreeNode(int t1, bool leaf1) {
    t = t1;
    leaf = leaf1;

    keys = new KeyType[2 * t - 1];
    values = new ValueType[2 * t - 1];
    C = new BTreeNode *[2 * t];

    n = 0;
}

template <typename KeyType, typename ValueType>
void BTreeNode<KeyType, ValueType>::traverse(std::vector<ValueType> &result) {
    int i;
    for (i = 0; i < n; i++) {
        if (leaf == false)
            C[i]->traverse(result);
        result.push_back(values[i]);
    }

    if (leaf == false)
        C[i]->traverse(result);
}

template <typename KeyType, typename ValueType>
void BTreeNode<KeyType, ValueType>::traverse() {
    int i;
    for (i = 0; i < n; i++) {
        if (leaf == false)
            C[i]->traverse();
        // std::cout << " " << keys[i] << ":" << values[i] << " ";
    }

    if (leaf == false)
        C[i]->traverse();
}

template <typename KeyType, typename ValueType>
BTreeNode<KeyType, ValueType> *BTreeNode<KeyType, ValueType>::search_node(KeyType k) {
    int i = 0;
    while (i < n && k > keys[i])
        i++;

    if (keys[i] == k)
        return this;

    if (leaf == true)
        return nullptr;

    return C[i]->search_node(k);
}

template <typename KeyType, typename ValueType>
ValueType BTreeNode<KeyType, ValueType>::search(const KeyType k) {
    BTreeNode *node = search_node(k);
    if (node) {
        auto it = std::lower_bound(node->keys, node->keys + node->n, k);
        int index = it - node->keys;
        return node->values[index];
    } else {
        return ValueType(0);
    }
}

template <typename KeyType, typename ValueType>
class BTree {
    BTreeNode<KeyType, ValueType> *root;
    int t;

   public:
    BTree(int _t) {
        root = nullptr;
        t = _t;
    }

    void traverse() {
        if (root != nullptr)
            root->traverse();
    }

    void traverse(std::vector<ValueType> &result) {
        if (root != nullptr) {
            root->traverse(result);
        }
    }

    ValueType search(const KeyType &k) {
        return (root == nullptr) ? ValueType(0) : root->search(k);
    }

    void insert(KeyType k, ValueType v);
};

template <typename KeyType, typename ValueType>
void BTree<KeyType, ValueType>::insert(KeyType k, ValueType v) {
    if (root == nullptr) {
        root = new BTreeNode<KeyType, ValueType>(t, true);
        root->keys[0] = k;
        root->values[0] = v;
        root->n = 1;
    } else {
        if (root->n == 2 * t - 1) {
            BTreeNode<KeyType, ValueType> *s = new BTreeNode<KeyType, ValueType>(t, false);

            s->C[0] = root;

            s->splitChild(0, root);

            int i = 0;
            if (s->keys[0] < k)
                i++;
            s->C[i]->insertNonFull(k, v);

            root = s;
        } else
            root->insertNonFull(k, v);
    }
}

int main() {
    BTree<string, string> t(3);
    t.insert("1", "1");
    t.insert("22", "22");
    t.insert("333", "333");

    cout << "The B-tree is: ";
    t.traverse();
}