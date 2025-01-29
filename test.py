x = [1,2]*(788//2)

print(len(x))

lst = []

for i in range(0,len(x)):
    lst.append(x[i:i+4])
    
flat_lst = [item for sublist in lst for item in sublist]
print(len(flat_lst))
