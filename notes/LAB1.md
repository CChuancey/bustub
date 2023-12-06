# LAB 1

## 说明

### 1. 可扩展HASH

lab的难点是理解可扩展hash的概念，以及插入的过程

**概念的区分：**

- GlobalDepth：表示当前**目录**要使用的最低几位哈希值的二进制的位数
- LocalDepth：表示每个**Bucket**中使用的最低几位哈希值的二进制的位数
- 目录大小：表示目录项的个数
- BucketSize：表示每个Bucket中存储元素的个数

BucketSize和LocalDepth不是一个东西，BucketSize在一开始就确定好了，而LocalDepth会随着元素的增多而增加

**插入的过程**
参考：[说明](https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/)

- 初始状态，GlobalDepth和LocalDepth为0，BucketSize是指定的参数，目录大小为1，目录中唯一项指向唯一一个Bucket
- 在插入元素时，通过计算Hash值，确定要插入的Bucket未满时，简单地将元素放入Bucket中
- 当要插入的Bucket中的元素满了，就需要下面的策略：
  - 如果要插入的Bucket的LocalDepth与GlobalDepth相同：增加GlobalDepth，目录项个数翻倍；
  - 如果要插入的Bucket的LocalDepth**小于**GlobalDepth（多个Dir项指向同一个Bucket）
    1. 增加LocalDepth
    2. 创建新的Bucket，将LocalDepth增加后，Hash值不再与LocalDepth-1时相同的元素转移到新的Bucket中
    3. 遍历目录指针，找到符合新增Bucket的位置处的指针

> 插入的Bucket满时，
> 情况1增加GlobalDepth，会导致情况2：多个Dir项指向同一个Bucket，需要进一步判断情况2
> 情况2创建新Bucket，也有可能出现原来n个元素的Bucket，split后，两个Bucket中元素个数分别是0和n的情况
> 情况1和情况2故要放在while循环中，直到插入成功！
