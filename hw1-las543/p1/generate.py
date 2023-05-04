import hashlib
import random

watermark = 'las543'
bitsW = bin(int(hashlib.sha256(watermark.encode()).hexdigest(), base=16)).lstrip('0b').zfill(256)[:16]
bytesW = int(bitsW, 2).to_bytes(len(bitsW) // 8, byteorder='big')
tempBytes = random.randbytes(6)

k = 4
n = 28

bins = [0] * (2**n)
coin = []

done = False
while not done:
    tempBytes = random.randbytes(6)
    testBytes = bytesW + tempBytes
    tempHash = bin(int(hashlib.sha256(testBytes).hexdigest(), base=16)).lstrip('0b').zfill(256)[:n]
    if bins[int(tempHash, 2)] == 0:
        bins[int(tempHash, 2)] = list([testBytes.hex()])
    else:
        bins[int(tempHash, 2)].append(testBytes.hex())
    if len(bins[int(tempHash, 2)]) == 4:
        coin = bins[int(tempHash, 2)]
        done = True
print(coin)
print("watermark: " + bitsW)


def forge(nid: str):
    watermark = nid
    ogHash = bin(int(hashlib.sha256(watermark.encode()).hexdigest(), base=16)).lstrip('0b').zfill(256)[:16]

    letters = 'abcdefghijklmnopqrstuvwxyz'
    digits = '0123456789'
    newHash = ""
    done = False

    while not done:
        for i in range(2, 4):
            letter = random.choices(letters, k=i)
            testList = letter
            for j in range(1, 11):
                digit = random.choices(digits, k=j)
                test = ''.join([str(elemL) for elemL in testList])
                test += ''.join([str(elemD) for elemD in digit])
                #print(test)
                newHash = bin(int(hashlib.sha256(test.encode()).hexdigest(), base=16)).lstrip('0b').zfill(256)[:16]
                if(ogHash == newHash):
                    forgedNid = test
                    done = True
                    break
    return forgedNid

def sum(a, b):
    return a + b

def sub(a, b):
    return a- b

#print("Forged watermark: " + forge("las543"))
