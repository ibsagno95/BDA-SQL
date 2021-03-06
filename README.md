# BDA-SQL
Le projet porte sur un système de gestion d'emprunt dans une librairie. Ce projet contient 5 tables:  
- Une table **Author** qui contient les noms des auteurs
- Une table **Book** qui contient l'identifiant , le titre et la catégorie du livre
- Une table **Student** qui contient les identifiants, les noms et les départements des étudiants
- Une table **Write** décrivant l'association entre les auteurs et les livres.
- Une table **Borrow** décrivant l'état des emprunts des livres.
# Instructions:  
Pour lancer le projet:  
:arrow_forward: Cloner le dépôt git sur votre ordinateur avec la l'instruction "git clone https://github.com/ibsagno95/BDA_emprunt".    
Le dépôt contient 3 repertoires et un fichier properties.conf: 
1. **script:** Contient le script python **projet_emprunt.py**.  
3. **Contention:** Contient la base de données exportée contenant l'état des emprunts (si rendu ou non ).
4. **output:** Contient une capture de quelques sorties.  
Et enfin un fichier **properties.conf** contenant les *paths* et un fichier **run.py** permettant de lancer le projet.

:arrow_forward: Se placer dans le repertoire cloné (**BDA_emprunt**) et ouvrir une console et taper  l'instruction:  
"spark-submit run.py"

Les réponses attendues appaîtront progressivement. Voici un exemple de sorties.
## Les étudiants qui ont emprunté le livre bid=’0002’ 
![](https://github.com/ibsagno95/BDA-SQL/blob/main/Output/emprunt_0002.PNG)  

## Les livres jamais empruntés
![](https://github.com/ibsagno95/BDA-SQL/blob/main/Output/livres_jamais_emprunte.PNG)  
**NB**: Les caractères spéciaux peuvent rencontrés des problèmes d'affichage.

## Etat des emprunts des livres (1 si supérieur à 3 mois 0 sinon)
![](https://github.com/ibsagno95/BDA-SQL/blob/main/Output/etat_des_emprunts.PNG)   

