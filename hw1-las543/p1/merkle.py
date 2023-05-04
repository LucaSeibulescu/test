from typing import Optional, List
from hashlib import sha256

def verify(obj: str, proof: str, commitment: str) -> bool:
	proof = proof.split(", ")
	hash = sha256(obj.encode()).hexdigest()
	if(len(proof) == 1):
		if(sha256((sha256(obj.encode()).hexdigest() + proof[0][:-1]).encode()).hexdigest() == commitment):
			hash = sha256((sha256(obj.encode()).hexdigest() + proof[0][:-1]).encode()).hexdigest()
		elif(sha256((sha256(obj.encode()).hexdigest()).encode()).hexdigest() == commitment):
			hash = sha256((sha256(obj.encode()).hexdigest()).encode()).hexdigest()
		else:
			hash = sha256((proof[0][:-1] + sha256(obj.encode()).hexdigest()).encode()).hexdigest()
	else:
		for i in range(len(proof)):
			if(int(proof[i][-1]) % 2 == 0):
				hash = sha256((hash + proof[i][:-1]).encode()).hexdigest()
			else:
				hash = sha256((proof[i][:-1] + hash).encode()).hexdigest()
	return hash == commitment

class Prover:
	def __init__(self):
		pass
	
	# Build a merkle tree and return the commitment
	def build_merkle_tree(self, objects: List[str]) -> str:
		odd = False
		if len(objects) % 2 != 0:
			odd = True
		self.objects = objects
		tempTree = []
		tree = []
		tempRow = []
		if(len(objects) == 1):
			tempRow.append(sha256(objects[0].encode()).hexdigest())
			tree.append(tempRow)
			tempRow = []
			tempRow.append(sha256((sha256(objects[0].encode()).hexdigest()).encode()).hexdigest())
			tree.append(tempRow)
			self.tree = tree
			return tree[len(tree)-1][0]
		for i in range(len(objects)):
			tempTree.append(sha256(objects[i].encode()).hexdigest())
			tempRow.append(sha256(objects[i].encode()).hexdigest())
		flag = []
		if(odd):
			tempRow.append(sha256(objects[-1].encode()).hexdigest())
			tempTree.append(sha256(objects[-1].encode()).hexdigest())
			flag.append(0)
		tree.append(tempRow)
		tempRow = []
		done = False
		idx = 1
		level = 1
		while not done:
			firstElem = tempTree.pop(0)
			secondElem = tempTree.pop(0)
			if((len(tempTree) == 0) or (len(objects) == 2)):
				tempRow = []
				tempRow.append(sha256((firstElem + secondElem).encode()).hexdigest())
				tree.append(tempRow)
				done = True
				break
			tempRow.append(sha256((firstElem + secondElem).encode()).hexdigest())
			tempTree.append(sha256((firstElem + secondElem).encode()).hexdigest())
			if(idx == (len(tree[level - 1]) // 2)):
				if(len(tempRow)%2 != 0):
					tempRow.append(tempRow[-1])
					tempTree.append(tempTree[-1])
					flag.append(level)
				tree.append(tempRow)
				tempRow = []
				level += 1
				idx = 0
			idx += 1
		for i in flag:
			tree[i].pop()
		tree.reverse()
		self.tree = tree
		#print(tree)
		return tree[0][0]

	def get_leaf(self, index: int) -> Optional[str]:
		if((index >= len(self.objects)) or (self.tree[-1][index] == '') or (index < 0)):
			return None
		else:
			return self.objects[index]
	
	def generate_proof(self, index: int) -> Optional[str]:
		tempIndex = index
		if tempIndex < 0 or tempIndex >= len(self.tree[-1]):
			return None
		if(self.get_leaf(index) != None):
			proof = []
			for i in range((len(self.tree)-1), 0, -1):
				if(tempIndex % 2 == 0):
					if tempIndex + 1 < len(self.tree[i]):
						proof.append(self.tree[i][tempIndex+1] + str(2))
					else:
						proof.append(str(2))
				else:
					proof.append(self.tree[i][tempIndex-1] + str(1))
				tempIndex = tempIndex // 2
			return ", ".join(proof)
		else:
			return None

'''
test = Prover()
tree = test.build_merkle_tree(['a', 'b', 'c'])
print(tree)
hash1 = sha256(('a' + 'b').encode()).hexdigest()
hash2 = sha256(('c' + 'd').encode()).hexdigest()
hash3 = sha256(('e' + '').encode()).hexdigest()
inter1 = sha256((hash1 + hash2).encode()).hexdigest()
inter2 = sha256((hash3 + '').encode()).hexdigest()
root = sha256((inter1 + inter2).encode()).hexdigest()
verify(test.get_leaf(0), test.generate_proof(0), tree)
'''
