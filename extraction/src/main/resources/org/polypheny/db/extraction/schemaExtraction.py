import sys

print("Hello, World!")
print("\n")
print("\nInput file:", sys.argv[1])
print("\nOutput file:", sys.argv[2])

f = open(sys.argv[2], "a")
f.write("The result!")
f.close()