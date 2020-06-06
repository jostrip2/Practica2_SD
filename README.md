# Sistemes Distribuïts 
## Pràctica 2: Mutual Exclusion
### Introducció
L'objectiu d'aquesta aplicació és implementar una versió distribuïda d'un algorisme d'exclusió mutua. 
En aquesta pràctica, l’exclusió mutua s’utilitza per protegir l’accés d’un fitxer compartit emmagatzemat a l’IBM COS. 

Hi haurà una sola funció que anomenarem “master” i múltiples funcions “slave” que s’executaran en paral·lel. 
Les funcions “slave” demanaran permís d’escriptura i la funció “master” ho gestionarà per a garantir exclusió mútua.

### Requisits
Per a poder executar aquesta aplicació es necessita tindre diversos programes instal·lats. 
* L’algorisme s’ha creat utilitzant el llenguatge Python (especificament Python3)
* Pywren (Plug-in de Python. Instal·lat utilitzant “pip3.7”)
  - S’ha de tenir un usuari a IBM Cloud i configurar-lo pel plug-in (fitxer “.pywren_config”) 
  - https://github.com/pywren/pywren-ibm-cloud/tree/master/config

### Paràmetres
Abans d'executar l'aplicació, s'ha de configurar el bucket i el nombre de slaves que que demanaran permis d'escriptura.

### Autors
* Joan Mercé López
* Joel Solà López
