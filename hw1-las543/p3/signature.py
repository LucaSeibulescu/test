import string
import random
import hashlib

# return the hash of a string
def SHA(s: string) -> string:
    return hashlib.sha256(s.encode()).hexdigest()

# transfer a hex string to integer
def toDigit(s: string) -> int:
    return int(s, 16)

# generate 2^d (si^{-1}, si) pairs based on seed r
def KeyPairGen(d: int, r: int) -> dict:
    pairs = {}
    random.seed(r)
    for i in range(1 << d):
        cur = random.randbytes(32).hex()
        while cur in pairs:
            cur = random.randbytes(32).hex()
        pairs[cur] = SHA(cur)
    return pairs


class MTSignature:
    def __init__(self, d, k):
        self.d = d
        self.k = k
        self.treenodes = [None] * (d+1)
        for i in range(d+1):
            self.treenodes[i] = [None] * (1 << i)
        self.sk = [None] * (1 << d)
        self.pk = None # same as self.treenodes[0][0]


    # Populate the fields self.treenodes, self.sk and self.pk. Returns self.pk.
    def KeyGen(self, seed: int) -> string:
        keys = KeyPairGen(self.d, seed)
        keyVals = list(keys.values())
        tree = []
        tree.append(keyVals)
        if(len(keyVals) % 2 != 0):
            tree[0].append(tree[0][-1])
        idx = 0
        index = 0
        level = 1
        done = False
        row = []
        if(self.d == 1):
            parent = format(0, "b").zfill(256) + tree[0][0] + tree[0][0]
            row.append(SHA(parent))
            tree.append(row)
            done = True
        while not done:
            parent = format(idx, "b").zfill(256) + tree[level - 1][index] + tree[level - 1][index + 1]
            row.append(SHA(parent))
            idx += 1
            index += 2
            if(index >= len(tree[level - 1])):
                if(idx % 2 != 0):
                    tree[level - 1].append(tree[level - 1][-1])
                    idx -= 1
                    index -= 1
                    continue
                tree.append(row)
                idx = 0
                index = 0
                level += 1
                row = []
                if(level == self.d):
                    parent = format(idx, "b").zfill(256) + tree[level - 1][index] + tree[level - 1][index + 1]
                    row.append(SHA(parent))
                    tree.append(row)
                    done = True
                    break
        tree.reverse()
        self.treenodes = tree
        self.sk = list(keys.keys())
        self.pk = self.treenodes[0][0]
        return self.pk

    # Returns the path SPj for the index j
    # The order in SPj follows from the leaf to the root.
    def Path(self, j: int) -> string:
        curr = j
        path = []
        for i in range(self.d, 0, -1):
            if(curr % 2 == 0):
                if(curr >= (len(self.treenodes[i]) - 1)):
                    path.append(self.treenodes[i][-1])
                else:
                    path.append(self.treenodes[i][curr + 1])
            else:
                path.append(self.treenodes[i][curr - 1])
            curr = curr // 2
        return ''.join(path)

    # Returns the signature. The format of the signature is as follows: ([sigma], [SP]).
    # The first is a sequence of sigma values and the second is a list of sibling paths.
    # Each sibling path is in turn a d-length list of tree node values. 
    # All values are 64 bytes. Final signature is a single string obtained by concatentating all values.
    def Sign(self, msg: string) -> string:
        z = []
        keys = []
        paths = []
        for j in range(1, self.k + 1):
            z.append(toDigit(SHA(format(j, "b").zfill(256) + msg)) % (2**self.d))
        for i in z:
            keys.append(self.sk[i])
        for i in z:
            paths.append(self.Path(i))
        signature = ''.join(keys) + ''.join(paths)
        return signature

def sig_forge():
    sig = MTSignature(10, 2)
    sig.KeyGen(2023)
    signature = sig.Sign("How many stars do you think there are in the universe?")
    forged = ' at least!'
    testBytes = 100000000
    done = False
    i = 0
    while not done:
        testBytes += i
        if(sig.Sign(str(testBytes) + forged) == signature):
            forged = str(testBytes) + forged
            done = True
        i += 1
    return forged

'''
sig = MTSignature(10, 2)
sig.KeyGen(2023)
print(sig_forge())
print(sig.Sign("How many stars do you think there are in the universe?"))
print(sig.Sign(sig_forge()))
'''
