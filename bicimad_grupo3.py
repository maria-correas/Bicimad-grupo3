#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  9 10:46:10 2022

@author: mat
"""

from pyspark import SparkContext
import json
import sys
import matplotlib
import matplotlib.pyplot as plt


sc = SparkContext()


#funcion que lee una linea del rdd y lo transforma enuna tupla de los objetos que nos interesa estudiar 
def mapper(line):
    data = json.loads(line)
    _id = data['_id']
    estacion_salida = data['idunplug_station']
    estacion_llegada = data['idplug_station']
    franja_horaria = data['unplug_hourTime']
    tiempo_viaje = data['travel_time']
    tipo_usuario = data['user_type']
    rango_edad = data['ageRange']
    return _id, estacion_salida, estacion_llegada, franja_horaria, tiempo_viaje,tipo_usuario,rango_edad

#toma una tupla de elementos y se queda con la estación de llegada y el rango de edad 
def mapper_edad(line):
    estacion_llegada = line[2]
    rango_edad = line[6]
    return rango_edad, estacion_llegada

#toma una tupla de elementos y se queda con la estación de llegada y la franja horaria
def mapper_hora(line):
    franja_horaria = int(line[3]['$date'].split('T')[1][0:2]) #nos quedamos solo con la hora
    estacion_llegada = line[2]
    return franja_horaria, estacion_llegada    

#toma una tupla de elementos y se queda con la estación de llegada  y la de salida
def mapper_distrito(line):
    estacion_salida = line[1]
    estacion_llegada = line[2]
    return estacion_salida,estacion_llegada

#toma una tupla de elementos y se queda con la duracion del viaje
def mapper_duracion(line):
    tiempo_viaje = line[4]
    return '1',tiempo_viaje

#funcion auxiliar que dada una lista de tuplas devuelv una lista con una lista con los primeros elementos de las tuplas
    #y otra con los segundos. 
def crear_lista(lista):
    l1 = []
    l2 = []
    for i in lista:
        l1.append(i[0])
        l2.append(i[1])
    return [l1,l2]



#funcion que muestra la duracion media de los viajes 
def duracion(rdd_base):
    #calculamos la media de las duraciones de los viajes
    rdd_duracion= rdd_base.map(mapper_duracion).groupByKey().map(lambda x : (x[0],sum(list(x[1]))/len(list(x[1])))).collect()
    print('Mostramosla duracion media de trayecto')
    print(int(rdd_duracion[0][1]//60)) #devuelve la duración media en minutos
    
    
def edades(rdd_base):
    #agrupamos los usuarios por rango de edad 
    rdd_edad = rdd_base.map(mapper_edad).groupByKey().mapValues(len).collect()
    print('Mostramos la cantidad de usuarios por rango de edad que hay: ')
    print(list(rdd_edad))
    
    #GRÁFICA DE BARRAS 
    ejes = crear_lista(list(rdd_edad))
    matplotlib.pyplot.bar(ejes[0],ejes[1])
    matplotlib.pyplot.ylabel('Número de usuarios')
    matplotlib.pyplot.xlabel('Rangos de Edad')
    plt.title('Número de usuarios dependiendo de la edad')
    matplotlib.pyplot.show()
    
    
    #GRAFICO DE SECTORES 
    labels = ejes[0]
    sizes = ejes[1]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax1.axis('equal')  
    plt.show()


def horarios(rdd_base):
    #agrupamos los usuarios por hora de salida 
    rdd_horas= rdd_base.map(mapper_hora).groupByKey().mapValues(len).collect()
    print('Mostramos la cantidad de usuarios en función de la hora: ')
    l = sorted(rdd_horas)
    print(l)
    ejes = crear_lista(list(l))
    
    #GRÁFICA DE LINEAS
    fig, ax = plt.subplots()
    ax.fill_between(ejes[0], ejes[1])
    plt.show()



#funcion que asocia a cada estación de salida su distrito  
def asociar_distrito(tupla):
    if tupla[0] <= 63 and tupla[0] >= 1:
        nodo = 'Centro'
    elif (tupla[0] >= 116 and tupla[0] <= 126) or tupla[0] == 132:
        nodo = 'Moncloa'
    elif tupla[0] >= 139 and tupla[0] <= 143:
        nodo = 'Tetuán'
    elif tupla[0] >= 156 and tupla[0] <= 161:
        nodo = 'Chamberí'
    elif (tupla[0] >= 144 and tupla[0] <= 155) or (tupla[0] >= 168 and tupla[0] <= 173) or (tupla[0]== 137) or (tupla[0]==138):
        nodo = 'Chamartin'
    elif (tupla[0] >= 92 and tupla[0] <= 115) or (tupla[0] >= 162 and tupla[0] <= 167):
        nodo = 'Salamanca'
    elif (tupla[0] >= 64 and tupla[0] <= 91):
        nodo = 'Retiro'
    else:
        nodo = 'Arganzuela'
    return nodo,tupla[1]


def distritos(rdd_base):
    #agrupamos los usuarios por distrito de salida 
    rdd_distrito = rdd_base.map(mapper_distrito).map(asociar_distrito).groupByKey().mapValues(len).collect()
    print('Mostramos la lista de segun el distrito: ')
    print(rdd_distrito)
    
   #GRÁFICA DE BARRAS 
    ejes = crear_lista(list(rdd_distrito))
    matplotlib.pyplot.bar(ejes[0],ejes[1])
    matplotlib.pyplot.ylabel('Número de usuarios')
    matplotlib.pyplot.xlabel('Distritos')
    plt.title('Número de usuarios en función del distrito de salida')
    matplotlib.pyplot.show()
    
    
    #GRAFICO DE SECTORES 
    labels = ejes[0]
    sizes = ejes[1]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%',startangle=90)
    ax1.axis('equal')  
    plt.show()


#sometemos al rdd a los distintos estudios
def proceso(rdd_base):
    duracion(rdd_base)
    edades(rdd_base)
    distritos(rdd_base)
    horarios(rdd_base)
    

#funcion que devuelve si la estación de llegada se ncuentra entre las que nos interesa estudiar o no 
def aux(x):
    lista = [116, 117,118,168,160,119]
    return x in lista


def main(sc,months):
    rdd = sc.parallelize([])
    for m in months:
        m = int(m)
        if m<10:
            filename = f"20190{m}_Usage_Bicimad.json"
            rdd1 = sc.textFile(filename)
            #filtramos para quedarnos solo con los viajes que llegan a una de las estaciones que pretendemos estudiar
            rdd_or = rdd1.map(mapper).filter(lambda x: aux(int(x[2]))) 
        else:
            filename = f"2019{m}_Usage_Bicimad.json"
            rdd1 = sc.textFile(filename)
            rdd_or = rdd1.map(mapper).filter(lambda x: aux(int(x[2])))
        rdd=rdd.union(rdd_or) #unimos los rdds 
    proceso(rdd)
    

if __name__ =="__main__":
    if len(sys.argv) < 2:
        months = [5]
    else:
        months=sys.argv[1].split(",") 

    main(sc, months)
    






