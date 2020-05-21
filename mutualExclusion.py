import pywren_ibm_cloud as pywren
import os
import pickle
import time

bucket = "sd-python"

N_SLAVES = 10

def master(id, x, ibm_cos):
    write_permission_list = []
    
    objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
    obj2 = list()
    for obj in objects:
        if "p_write_" in obj['Key']:                                    # List all "p_write_{id}" files
            obj2.append(obj)
    
    while len(obj2) > 0:                                                # until no "p_write_{id}" objects in the bucket
        obj2.sort(key=lambda k: k['LastModified'], reverse=True)        # Order objects by time of creation
    
        elem = obj2.pop(0)                                              # Pop first object of the list "p_write_{id}"
        idElem = elem['Key'][9:]

        f = open("write_"+idElem, "wb+")                                # Write empty "write_{id}" object into COS
        ibm_cos.put_object(Bucket=bucket, Key="write_"+idElem, Body=f)

        ibm_cos.delete_object(Bucket=bucket, Key="p_write_"+idElem)     # Delete from COS "p_write_{id}"
        write_permission_list.append(idElem)                            # save {id} in write_permission_list

    # 7. Monitor "result.json" object each X seconds until it is updated
        #json = ibm_cos.get_object(Bucket=bucket, Key='result.json')['Body'].read()

        ibm_cos.delete_object(Bucket=bucket, Key="write_"+idElem)       # Delete from COS “write_{id}”
        
        time.sleep(x)                                                   # monitor COS bucket each X seconds
        objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
        obj2 = list()
        for obj in objects:
            if "p_write_" in obj:
                obj2.append(obj)

    return write_permission_list


def slave(id, x, ibm_cos):
    # Write empty "p_write_{id}" object into COS
    f = open("p_write_"+str(id), "wb+")
    ibm_cos.put_object(Bucket=bucket, Key="p_write_"+str(id), Body=f)

    # Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
    continuar = True
    for obj in objects:
        if "write_"+str(id) in obj['Key']: continuar = False
    while (continuar):
        objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
        continuar = True
        for obj in objects:
            if "write_"+str(id) in obj['Key']: continuar = False
        time.sleep(x)
    
    # If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    resultSer = ibm_cos.get_object(Bucket=bucket, Key='result.txt')['Body'].read() 
    result = pickle.loads(resultSer)
    result.append(str(id)+" ")
    resultSer = pickle.dumps(result)
    ibm_cos.put_object(Bucket=bucket, Key="result.txt", Body=resultSer)



if __name__ == '__main__':

    pw = pywren.ibm_cf_executor()
    pw.call_async(master, 0)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    
    ibm_cos = pw.internal_storage.get_client()
    result = ibm_cos.get_object(Bucket=bucket, Key='result.txt')['Body'].read()         # Get result.txt
    result = result.decode()

    # check if content of result.txt == write_permission_list
    separat = result.split() 
    for i in len(separat):
        print(separat[i]+" "+write_permission_list[i]+"\n")
        if separat[i] != write_permission_list[i]:
            print("No s'ha escrit en el mateix ordre.\n")

