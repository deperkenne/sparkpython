"""

BaseException
├── SystemExit
├── KeyboardInterrupt
├── GeneratorExit
└── Exception
    ├── ArithmeticError
    │   ├── ZeroDivisionError
    │   ├── OverflowError
    │   └── FloatingPointError
    ├── LookupError
    │   ├── IndexError
    │   └── KeyError
    ├── ImportError
    │   └── ModuleNotFoundError
    ├── OSError
    │   ├── FileNotFoundError
    │   ├── PermissionError
    │   └── TimeoutError
    ├── TypeError
    ├── ValueError
    ├── AttributeError
    ├── NameError
    │   └── UnboundLocalError
    ├── RuntimeError
    ├── MemoryError
    ├── AssertionError
    └── EOFError

"""

# runtimeError lorsqu'on a un probleme de reference circulaire exemple une boucle infinie ou une fonction recursive qui se return elle meme
# runtimeError mauvaise utilisations d'une bibliotheque
# IOError : se produit lorsqu'il y'a un probleme d'entree/sortie lors de l'acces a des fichierou ressource externe (operation de lecture et d'ecriture
# IOError exemple: (espace disque insuffisant, Permissions insuffisantes ,ouverture d'un fichier qui n'existe pas avec un mode "r"

list = [x for x in range(1,4)]

print(dir(list))
print(list.reverse()) # renvoie la list renverser et ne retourne rien
print(list)
print(list.remove(1)) # supprime l'element 1 et ne retourne rien
print(list)
list.insert(1,8)
print(list)
print(list.index(2)) #  nous donne l'index de lelement 2
print(list.pop()) # pop supprime  et renvoie le drnier element de la list
print(list.count(2)) # counte le nombre d'apparition de l'element 2 dans la liste


class A :
    def __init__(self, value = 2):
        self.value = value
        print("la vale",value)
class B(A):
  pass  # sans constructeur de B le constructeur de A est appele par defaut
        # une fois qu'on definit le constructeur de B nous devons appeler le constructeur de A qui peut se faire
        # de 2 maniere soit avec A.__init__(self) ou super().__init__() ou super().__init__(avec valeur s'il contient)

class C(A):
    def __init__(self, valu=3):
        super().__init__(valu)
        self.valu = valu
b = B()
c = C()

def batch_data(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size] # collector ou generateur

# Utilisation
data = [1, 2, 3, 4, 5, 6, 7, 8, 9]
for batch in batch_data(data, batch_size=3):
    print(batch) # shared kinesis

# ceration d'une fonction pour remplir un dicko

dic = {}
i = 0
print(dir(dic))

def full_dic(key,obj):

    dic.setdefault(key,obj) # ici on insere une cle et un object comme valeur dans un dict

full_dic("kenne",c)
full_dic("yann",c)
dic.pop("kenne") # supprime l'element avec la cle "kenne"
dic.update(yann=b)


for key,val in  dic.items() :
    print(key, val.value)

# Exemple de mise à jour avec un autre dictionnaire
dict1 = {"a": 1, "b": 2}
dict2 = {"b": 3, "c": 4}
dict1.update(dict2)


dict1.popitem() # suprime le dernier item dans le dictionaire
print(dict1.get("a")) # nous donne la valeur qui se trouve dans la cle
print(dict1)

# Exemple de mise