import ast
#from asyncio.windows_events import NULL
import collections
from gc import collect
from itertools import product
#from math import prod
from pprint import pprint
from pydoc import cli
from sqlite3 import Cursor
from xml.dom.minidom import Document
import pymongo
from bson.objectid import ObjectId
from bson.code import Code
#from pymongo.server_api import ServerApi
from pymongo import MongoClient, errors
import json
import time
import datetime
import re


from pymongo import MongoClient
from bson.objectid import ObjectId

c=MongoClient("mongo2.iem",port=27017,username="al800546",password="al800546",authSource="al800546",authMechanism="SCRAM-SHA-1")
db=c.al800546
'''
try:
    start = time.time()
    #client = pymongo.MongoClient("mongodb+srv://avataxis:5GMF1JZ3CPK7SdWy@cluster0.4svgv.mongodb.net/SITE_JEUX?retryWrites=true&w=majority",server_api=ServerApi('1'))
    client = pymongo.MongoClient("mongodb+srv://avataxis:MYuGQrUUgc4F5bJz@cluster0.4svgv.mongodb.net/SITE_JEUX?retryWrites=true&w=majority", server_api=ServerApi('1'))
    db = client.get_database('SITE_JEUX')
except:
    print ("connection error")
    # print le temps ecoule
    print (time.time() - start)
'''

def add_producer(nom="producer",siege=[0,"rue","cp","ville","pays"], filiales=[]):
    producteur = db['producteur']
    producteur.insert_one({
        "nom":nom,
        "siege":{
            "num":siege[0],
            "rue":siege[1],
            "cp":siege[2],
            "ville":siege[3],
            "pays":siege[4]
        },
        "filiales":filiales
    })
    #--une verification d'insertion ici

    print("Producteur ",nom," est ajoute avec succes!")


def add_dlc(nom="dlc",prix=0.0,descr="description",date=datetime.datetime.now(), jeu="jeu"):
    cursor_jeu = db.jeu.find_one({"nom":jeu},{"nom":1,"plateforme":1,"description":1,"_id":0})
    if cursor_jeu is None:
        print("Le jeu n'existe pas! insertion du dlc sans jeu")

    dlc = db['dlc']
    res = dlc.insert_one({
        "nom":nom,
        "prix":prix,
        "description":descr,
        "date_sortie":date,
        "jeu":cursor_jeu
    })

    print("DLC ",nom," est ajoute avec succes!")


def add_jeu(nom="jeu",descr="description",prix=0.0,date_s=datetime.datetime.now(),
date_v=datetime.datetime.now(),stock=0,plateforme=[],multi=False,retro=False,
langue=["ENG-us"],producer="producer",nbavis=0,note=0,dlc=["dlc","hello"],cat_main="Aventure",cats=["FPS","RPG"]):

    #recup le nom et id du producteur
    cursor_prod=db.producteur.find_one({"nom":producer},{"nom":1,"_id":1})
    if cursor_prod is None:
        print("Le producteur n'existe pas! insertion du jeu sans producteur")

    #recup les noms prix ids du/des dlcs
    dlcs=[]
    if len(dlc)!= 0: #si le tableau dlc est pas vide
        if len(dlc) > 1: #si dlcs contient >1 noms
            cursor_dlcs = db.dlc.find({"nom":{"$in":dlc}},{"nom":1,"prix":1})
            for doc in cursor_dlcs:
                dlcs.append(doc)
        else: #si dlcs contient 1 nom
            cursor_dlcs = db.dlc.find_one({"nom":dlc[0]},{"nom":1,"prix":1})
            dlcs.append(cursor_dlcs)

    #recup les ids des categories
    if len(cats)!=0:
        if len(cats) >1:
           cursor_categories= db.categorie.find({"nom":{"$in":cats}},{"_id":1})
        else:
            cursor_categories= db.categorie.find_one({"nom":cats[0]},{"_id":1})
    else:
        print("Le tableau des categories est vide")
    #mettre les dictionnaires des categories dans le tableau
    categories = []
    for doc in cursor_categories:
        categories.append(doc)

    #insertion
    jeu = db['jeu']
    jeu.insert_one({
      "nom":nom,
      "description":descr,
      "prix":prix,
      "date_sortie":date_s,
      "mise_en_vente":date_v,
      "stock":stock,
      "plateforme":plateforme,
      "multijoueur":multi,
      "retro":retro,
      "langue":langue,
      "producteur":cursor_prod,
      "Dlc":dlcs,
      "categorie_principale":cat_main,
      "categories":categories,
      "nb_avis":nbavis,
      "note":note
    })


def add_categorie(nom="categorie",descr="description",cats=["FPS"],parent="Aventure"):
    cursor_categories= db.categorie.find({"nom":{"$in":cats}})
    sous_cats = []

    for doc in cursor_categories:
        sous_cats.append(doc)

    cursor_parent=db.categorie.find_one({"nom":parent},{"nom":1,"_id":1})

    #insertion
    categorie = db['categorie']
    categorie.insert_one({
        "nom":nom,
        "description":descr,
        "sous_categories":sous_cats,
        "parent":cursor_parent
    })


def add_client(pseudo="pseudo_client",email="client@email.com",nom="nom_client",prenom="prenom_client",tel="",mdp="",
adresse=[[0,"rue1","cp1","ville1","pays1"],[1,"rue2","cp2","ville2","pays2"]]):

    #mettre les adresses dans un dictionnaire (form json)
    adresse_dict=[]
    for a in adresse:
        adresse_dict.append(
            {
                "num":a[0],
                "rue":a[1],
                "cp":a[2],
                "ville":a[3],
                "pays":a[4]
            }
        )
    user = db['user']
    user.insert_one({
        "pseudo":pseudo,
        "email":email,
        "nom":nom,
        "prenom":prenom,
        "telephone":tel,
        "mot_de_passe":mdp,
        "adresse":adresse_dict
    })


def add_avis(note=0.0,com="commentaire",date=datetime.datetime.now(),jeu="jeu",pseudo_client="pseudo_client"):
    cursor_user = db.user.find_one({"pseudo":pseudo_client},{"pseudo":1})
    if cursor_user is None:
        print("Le client n'existe pas! impossible de mettre un avis.")
        return

    cursor_jeu = db.jeu.find_one({"nom":jeu})

    if cursor_jeu == None:
        pprint("Le jeu n'existe pas! impossible de mettre un avis.")
        return

    avis=db['avis']
    avis.insert_one({
        "note":note,
        "commentaire":com,
        "date":date,
        "client":cursor_user,
        "jeu": jeu
    })

def add_commande(date_livraison=datetime.datetime.now(),etat="",total=0.0,client="pseudo_client",jeux=["jeu","The Witcher 3: Wild Hunt"]):
    cursor_user = db.user.find_one({"pseudo":client},{"_id":1})
    if cursor_user is None:
        print("Le client n'existe pas! impossible d'enregister la commande.")
        return

    cursor_jeux = db.jeu.find({"nom":{"$in":jeux}},{"nom":1,"prix":1,"_id":0})
    jeux_dict = []
    for doc in cursor_jeux:
        jeux_dict.append(doc)

    commande=db['commande']
    commande.insert_one({
        "date_livraison":date_livraison,
        "etat":etat,
        "prix_total":total,
        "client":cursor_user,
        "jeux":jeux_dict
    })


def remove_document(col="producteur",nom="producer", id=""):
    collection = db[col]
    res = collection.delete_one({"$or":[{"nom":nom},{"_id":id}]})
    if res.deleted_count == 1:
        print("document supprime avec success!")
    else:
        print(res._raise_if_unacknowledged)


def maj_document(col="producteur", where={"nom":"producer"} ,fields={"nom":"nouveau producer"}):
    collection = db[col]
    res = collection.update_one(where,{"$set":fields})
    if res.modified_count == 1:
        print("Document mis a jour avec success!")
    else:
        print(res._raise_if_unacknowledged)


def menu():
    print("MENU:")
    print('1) Afficher le contenu de la collection jeu')
    print('2) Afficher le contenu de la collection categorie')
    print('3) Afficher le contenu de la collection producteur')
    print('4) Afficher le contenu de la collection dlc')
    print('5) Afficher le contenu de la collection commande')
    print('6) Afficher le contenu de la collection avis')
    print('7) Afficher le contenu de la collection client')
    print('8) le nom, prix, stock de tous les jeux qui se joue sur une plteforme passe en parametr')
    print('9) tous les jeux cree par un producteur particulier')
    print('10)toutes les personnes qui ont commande un jeu')
    print('11) afficher le top 10 des jeux les mieux note')
    print('12) compter le nombre de jeux qui nont pas de DLC')
    print('13) le producteur qui a le plus develope un jeu et le nombre de ses jeux produits en ordre croissant')
    print('14) compter de commande passe par jour')

    x= input('Entrer votre choix:')
    x=int(x)
    res={}
    if int(x) == 1:
         res = db.jeu.find()
    elif x==2:
        res = db.categorie.find()
    elif x==3:
        res = db.producteur.find()
    elif x==4:
        res=db.dlc.find()
    elif x==5:
        res=db.commande.find()
    elif x==6:
        res=db.avis.find()
    elif x==7:
        res=db.client.find()
        print("###################### REQUETES ######################")
    elif x==8:
        p = input("Entrer le plateforme:")
        res=db.jeu.find({"plateforme":p},{"nom":1,"prix":1,"stock":1})
    elif x==9:
        p=input("Entrer un producteur:")
        res=db.jeu.find({"producteur.nom":p},{"producteur":0})
    elif x==10:
        j = input("Entrer un jeu:\n")
        res=db.commande.aggregate([
            {
                "$match" : {"jeux.nom":j}
            },
            {
                "$lookup":
                {
                    "from":"user",
                    "localField":"client._id",
                    "foreignField":"_id",
                    "as":"liste_acheteur_jeu"
                }
            }
            ])
    elif x==11:
        res=db.jeu.find({},{"nom":1,"nb_avis":1,"note":1}).sort("note",-1).limit(10)
    elif x==12:
        res= db.jeu.count_documents({"$or":[{"Dlc":[]},{"Dlc":{"$exists":False}}]})
    elif x==13:
        res= db.jeu.aggregate([
            {
                "$group":{"_id":"$producteur","total":{"$sum":1}}
            },
            {
                "$project":{"producteur.nom":1,"total":1}
            },
            {
                "$sort":{"total":1}
            }
        ])
    elif x==14:
        res= db.commande.aggregate([
            {
                "$group":{"_id":"$date_livraison","total":{"$sum":1}}
            }
        ])
    else:
        print("ignored")
    pprint(list(res))
#menu()
#db.user.update_one({"age":{"$exists":True}},{"$set":{"age":16}})

#regex = re.compile("Hitman *",re.IGNORECASE)

################# QUERIES #################
'''
map =Code("function() {"
            "   commande.jeux.foreach("
            "       function(j){"
            "           emit(z,1);"
            "       }"
            "   );"
            "}"
        )
reduce = Code("function(key,values){"
                    "var total=0;"
                    "for(var i=0; i<values.length;i++){"
                    "   total+=values[i];"
                    "}"
                    "return total;"
)

db.commande.map_reduce(map,reduce,"occurence_jeux")
'''

#tous les avis emises par une personne donne
'''
pseudo = input('Entrer le pseudo du client:\n')
res=db.avis.aggregate([
    {
        "$match":{"client.pseudo":pseudo}
    },
    {
        "$lookup":{
            "from":"user",
            "localField":"client._id",
            "foreignField":"_id",
            "as":"liste_avis"
        }
    },
    {
        "$project":{"note":1,"commentaire":1,"date":1}
    }
])'''

#la somme des achats d'une personne en euro
'''
pseudo=input("Entrer le pseudo du client:\n")
res=db.commande.aggregate([
    {
        "$lookup":{
            "from":"user",
            "localField":"client._id",
            "foreignField":"_id",
            "as":"user_data"
        }
    },
    {
        "$match":{"user_data.pseudo":pseudo}
    },
    {
        "$group":{
                    "_id":"",
                    "prix_total":{"$sum":"$prix_total"},
                    "count":{"$sum":1}
                }
    },
    {
        "$project":{"_id":0,"nbr_commandes":"$count","total_achats":"$prix_total"}
    }
])
'''
#Decrire une requete permettant le calcul du nombre de produits de chaque categorie, en considerant toutes les
#categories auxquelles le produit peut appartenir, et en creant une collection nommee  countsByCategory .
'''
res=db.jeu.aggregate([
    {
        "$unwind":"$categories"
    },
    {
        "$group":{"_id":"$categories","total":{"$sum":1}}
    },
    {
        "$project":{"_id":1,"total":1}
    }
])

pprint(list(res))
'''


map= Code("function(){"
    "var value={nb:1};"
    "emit(this.age, value);"
    "}")

reduce= Code("function(age,val){"
    "var resultat={nb:0};"
    "for(var i=0;i<val.length;i++){"
        "resultat.nb+=val[i].nb;"
    "}"
    "return resultat;"
    "}")

res=db.user.map_reduce(map,reduce,"user_age")
res=db.user_age.find({})
pprint(list(res))