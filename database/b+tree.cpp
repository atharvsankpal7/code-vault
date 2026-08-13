#include <bits/stdc++.h>
using namespace std;
#define maxKeys 4
#define minKeys 2
struct TreeNode
{
    bool isLeaf = true;
    vector<int> keys;
    vector<unique_ptr<TreeNode>> child;
    TreeNode *siblingLarge = nullptr;
    TreeNode *siblingSmall = nullptr;
    TreeNode *parent = nullptr;
    vector<int> values;
};

unique_ptr<TreeNode> root = nullptr;

int search(TreeNode *node, int target)
{
    if (node == nullptr)
        return -1;

    if (node->isLeaf)
    {
        for (size_t i = 0; i < node->keys.size(); i++)
        {
            if (node->keys[i] == target)
            {
                return node->values[i];
            }
        }
        // no key match
        return -1;
    }

    int i = 0;
    while (i < node->keys.size())
    {
        if (node->keys[i] > target)
        {
            return search(node->child[i].get(), target);
        }
        i++;
    }
    return search(node->child[i].get(), target);
}

int getInsertPosition(int &key, vector<int> &keys)
{
    int i = 0;
    while (i < keys.size() && keys[i] < key)
    {
        i++;
    }
    return i;
}
void split(unique_ptr<TreeNode> &node)
{
    int promotionalKey; // key tobe promoted to the parent as divider usually the middle key
    int mid = node->keys.size() / 2;
    promotionalKey = node->keys[mid];
    auto sibling = make_unique<TreeNode>();
    if (node->isLeaf)
    {
        sibling->isLeaf = true;
        sibling->keys.assign(node->keys.begin() + mid, node->keys.end());
        sibling->values.assign(node->values.begin() + mid, node->values.end());
        node->keys.erase(node->keys.begin() + mid, node->keys.end());
        node->values.erase(node->values.begin() + mid, node->values.end());

        sibling->siblingLarge = node->siblingLarge;
        node->siblingLarge = sibling.get();
        sibling->siblingSmall = node.get();
    }
    else // if node is not leaf
    {
        sibling->isLeaf = false;
        sibling->keys.assign(node->keys.begin() + mid, node->keys.end());
        node->keys.erase(node->keys.begin() + mid, node->keys.end());

        sibling->child.assign(node->child.begin() + mid + 1, node->child.end());
        node->child.erase(node->child.begin() + mid + 1, node->child.end());
    }

    // if node is root
    if (node->parent == nullptr)
    {
        auto newRoot = unique_ptr<TreeNode>();
        newRoot->isLeaf = false;
        newRoot->keys.push_back(promotionalKey);

        newRoot->child.push_back(node);
        newRoot->child.push_back(sibling);

        node->parent = newRoot.get();
        sibling->parent = newRoot.get();

        root = move(newRoot);
    }
    else // if node is inner node
    {
        TreeNode *parent = node->parent;
        sibling->parent = parent;

        int pos = getInsertPosition(promotionalKey, parent->keys);
        parent->keys.insert(parent->keys.begin() + pos, promotionalKey);
        parent->child.insert(parent->child.begin() + pos + 1, move(sibling));

        if (parent->keys.size() > maxKeys)
        {
            if (parent->parent == nullptr)
            {
                split(root); // create a new root
            }
            else
            {
                TreeNode *grandparent = parent->parent;
                int parentIdx = 0;
                while (parentIdx < grandparent->child.size())
                {
                    if (grandparent->child[parentIdx].get() == parent)
                    {
                        break;
                    }
                    parentIdx++;
                }
                split(grandparent->child[parentIdx]);
            }
        }
    }
}
void insert(unique_ptr<TreeNode> &node, int key, int value)
{
    if (node == nullptr)
    {
        node = make_unique<TreeNode>();
        node->keys.push_back(key);
        node->values.push_back(value);
        return;
    }
    else
    {
        if (node->isLeaf)
        {
            int i = getInsertPosition(key, node->keys);
            node->keys.insert(node->keys.begin() + i, key);
            node->values.insert(node->values.begin() + i, value);
            // if node is full
            if (node->keys.size() > maxKeys)
            {
                split(node);
            }
            return;
        }
        // if node is inner leaf
        else
        {
            int i = 0;
            while (i < node->keys.size())
            {
                if (node->keys[i] > key)
                {
                    return insert(node->child[i], key, value);
                }
                i++;
            }
            return insert(node->child[i], key, value);
        }
    }
}
