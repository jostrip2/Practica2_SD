import pywren_ibm_cloud as pywren
import json
import time

BUCKET_NAME = "sd-python"

N_SLAVES = 2

def master(id, x, ibm_cos):
    write_permission_list = []

    ibm_cos.put_object(Bucket=BUCKET_NAME, Key="result.json", Body=json.dumps([]))
    
    objects = []
    while len(objects) == 0:
        try:
            objects = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix="p_write")['Contents']
        except:
            time.sleep(x)
                    
    temps = ibm_cos.get_object(Bucket=BUCKET_NAME, Key="result.json")['LastModified']

    while len(objects) != 0:                                                               # until no "p_write_{id}" objects in the bucket      
        objects.sort(key=lambda k: k['LastModified'])                                      # Order objects by time of creation
        
        idElem = objects.pop(0)['Key'][8:]                                                 # Pop first object of the list "p_write_{id}"                                          
        
        ibm_cos.put_object(Bucket=BUCKET_NAME, Key="write_"+idElem, Body=json.dumps(""))   # Write empty "write_{id}" object into COS

        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key="p_write_"+idElem)                   # Delete from COS "p_write_{id}"
        write_permission_list.append(idElem)                                               # save {id} in write_permission_list
        
        temps2 = ibm_cos.get_object(Bucket=BUCKET_NAME, Key="result.json")['LastModified'] # Monitor "result.json" object each X seconds until it is updated
        while(temps == temps2):
            time.sleep(x)
            temps2 = ibm_cos.get_object(Bucket=BUCKET_NAME, Key="result.json")['LastModified']
        temps = temps2
        
        ibm_cos.delete_object(Bucket=BUCKET_NAME, Key="write_"+idElem)                       # Delete from COS “write_{id}”
        
        if len(objects) == 0:
            try:
                objects = ibm_cos.list_objects(Bucket=BUCKET_NAME, Prefix="p_write")['Contents']
            except:
                time.sleep(x)
            
    return write_permission_list



def slave(id, x, ibm_cos):
    # Write empty "p_write_{id}" object into COS
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key="p_write_"+str(id), Body=json.dumps(""))

    # Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    while True:
        try:
            _ = ibm_cos.get_object(Bucket=BUCKET_NAME, Key="write_"+str(id))
        except:
            time.sleep(x)
        else:
            break

    # If write_{id} is in COS:
    result = ibm_cos.get_object(Bucket=BUCKET_NAME, Key='result.json')['Body'].read()   # get result.json
    result = json.loads(result)
    result.append(str(id))                                                              # append {id}
    result = json.dumps(result)
    ibm_cos.put_object(Bucket=BUCKET_NAME, Key="result.json", Body=result)              # put back to COS result.json



if __name__ == '__main__':

    if N_SLAVES <= 100:
        pw = pywren.ibm_cf_executor()
        ibm_cos = pw.internal_storage.get_client()
        
        espera=[]
        for _ in range(N_SLAVES):
            espera.append(1)

        pw.call_async(master, 1)
        pw.map(slave, espera)
        write_permission_list = pw.get_result()[0]
        pw.clean()

        result = ibm_cos.get_object(Bucket=BUCKET_NAME, Key='result.json')['Body'].read()        # Get result.json
        result = json.loads(result)

        # check if content of result.json == write_permission_list
        ok = True
        print("\nResult:")
        print(result)
        print("Write permission:")
        print(write_permission_list)
        for i in range(len(result)):
            if result[i] != write_permission_list[i]:
                ok = False
        if ok: print("\nL'ordre és correcte\n")
        else: print("\nL'ordre NO és correcte\n")
    
    else: print("El nombre d'esclaus ha de ser inferior o igual a 100.")