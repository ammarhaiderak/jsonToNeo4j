import json
from typing import final
from neo4j import GraphDatabase
import re
import random

driver = GraphDatabase.driver('bolt://localhost:7687', auth=('test', 'test'))


def getfinalkey(trail):
    separator = "_"
    return separator.join(trail)

def getformated(x):
    my_new_string = re.sub('[^a-zA-Z0-9_]', '', x)
    # return x.replace(",","").replace(".","").replace("'","").replace("-","")
    return my_new_string

def getrandomid():
    return random.randint(24,10000)

def jsonToNeo_v4(payload, trail=""):
    global iidd
    
    keys = payload.keys()
    # obj = []
    obj = {}

    links = []
    for k in keys:
        # print(f'key: {k} links: {links} obj: {obj}')
        if type(payload[k]) is dict and len(payload[k].keys())>0:
            # trail = trail + k + '.'
            o, l = jsonToNeo_v4(payload[k])
            # print(trail)
            
            # create a node k with obj as props
            formated_key = getformated(k)
            final_key = f'{formated_key}_{getrandomid()}'
            print(f'key: {final_key} links: {l} obj: {o}')
            # iidd += 1
            o['__id']=final_key
            o['title']=formated_key
            o['name']=formated_key
            props = {
                "props": o
            }    

            query = f"""
                CREATE (n:{formated_key} $props)
            """
            relate = None
            st = []
            for rel in l:
                relate = f"""
                MATCH
                (a:{formated_key}),
                (b)
                WHERE a.__id = '{final_key}' AND b.__id = '{rel}'
                MERGE (a)-[r:RELATES_TO]->(b)
                RETURN a
                """
                st.append(relate)
            # print(st)
            with driver.session() as session:
                session.run(query, props)
                for s in st:
                    session.run(s)

            links.append(final_key) # telling parent that I am your link
            # links.append(o)
        elif type(payload[k]) is str:
            # obj.append({k:payload[k]})
            obj[k]=payload[k]

        elif type(payload[k]) is list and len(payload[k])>0:
            parent_node_id = f'{getformated(k)}_{getrandomid()}' 
            props = {
                "props": {
                    "__id": parent_node_id
                }
            }
            query = f"""CREATE (n:{getformated(k)} $props)"""
            with driver.session() as session:
                session.run(query, props)

            for item in payload[k]:
                newkey = f'{getformated(k)}_{getrandomid()}'
                o, l = jsonToNeo_v4({newkey: item})
                print(f'inside list case: key: {newkey} links: {l} obj: {o}')
                
                if len(l) == 0:
                    o['__id']=newkey
                    o['name']=getformated(k)
                    o['title']=getformated(k)
                    props = {
                        "props": o
                    }
                    
                    query = f"""
                    MATCH (n) WHERE n.__id = "{parent_node_id}" WITH n
                    CREATE (b:{getformated(k)} $props) -[:RELATES_TO]->(n)"""
                    with driver.session() as session:
                        session.run(query, props)

                else:
                    st = []
                    for rel in l:
                        relate = f"""
                        MATCH
                        (a:{getformated(k)}),
                        (b)
                        WHERE a.__id = '{parent_node_id}' AND b.__id = '{rel}'
                        MERGE (a)<-[r:RELATES_TO]-(b)
                        RETURN a
                        """
                        st.append(relate)

                    with driver.session() as session:
                        for s in st:
                            session.run(s)
                
            links.append(parent_node_id)

    return obj, links
