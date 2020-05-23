import pywren_ibm_cloud as pywren
import json
import time

bucket = "sd-python"

N_SLAVES = 2

def master(id, x, ibm_cos):
    write_permission_list = []
    
    obj2 = []
    while True:
        objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
        for obj in objects:
            if "p_write_" in obj['Key']:                                                # List all "p_write_{id}" files
                obj2.append(obj)
        if len(obj2) > 0: break
        obj2.clear()

    temps = ibm_cos.get_object(Bucket=bucket, Key="result.json")['LastModified']
    while True:                                                                         # until no "p_write_{id}" objects in the bucket      
        obj2.sort(key=lambda k: k['LastModified'])                                      # Order objects by time of creation

        idElem = obj2.pop(0)['Key'][8:]                                                 # Pop first object of the list "p_write_{id}"                                            

        #ibm_cos.put_object(Bucket=bucket, Key="write_"+idElem, Body=json.dumps(""))     # Write empty "write_{id}" object into COS

        ibm_cos.delete_object(Bucket=bucket, Key="p_write_"+idElem)                     # Delete from COS "p_write_{id}"
        write_permission_list.append(idElem)                                            # save {id} in write_permission_list
        
        temps2 = ibm_cos.get_object(Bucket=bucket, Key="result.json")['LastModified']   # Monitor "result.json" object each X seconds until it is updated
        while(temps == temps2):
            time.sleep(x+1)
            temps2 = ibm_cos.get_object(Bucket=bucket, Key="result.json")['LastModified']
        temps = temps2
        
        #ibm_cos.delete_object(Bucket=bucket, Key="write_"+idElem)                       # Delete from COS “write_{id}”
        
        time.sleep(x+1)

        objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
        obj2.clear()
        for obj in objects:
            if "p_write_" in obj:
                obj2.append(obj)
        if len(obj2) == 0: break

    return write_permission_list



def slave(id, x, ibm_cos):
    # Write empty "p_write_{id}" object into COS
    ibm_cos.put_object(Bucket=bucket, Key="p_write_"+str(id), Body=json.dumps(""))

    # Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    buscar = True
    while buscar:
        time.sleep(x+1)
        objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
        for obj in objects:
            if "write_"+str(id) in obj['Key']: 
                buscar = False
                break

    time.sleep(x+1)
    # If write_{id} is in COS:
    if not buscar:
        result = ibm_cos.get_object(Bucket=bucket, Key='result.json')['Body'].read()    # get result.json
        result = json.loads(result)
        result.append(str(id))                                                          # append {id}
        result = json.dumps(result)
        ibm_cos.put_object(Bucket=bucket, Key="result.json", Body=result)               # put back to COS result.json




if __name__ == '__main__':

    pw = pywren.ibm_cf_executor()
    
    ibm_cos = pw.internal_storage.get_client()
    ibm_cos.put_object(Bucket=bucket, Key="result.json", Body=json.dumps([]))

    pw.call_async(master, 0)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    
    result = ibm_cos.get_object(Bucket=bucket, Key='result.json')['Body'].read()        # Get result.json
    result = json.loads(result)

    print(write_permission_list)
    #print(write_permission_list[0] >= write_permission_list[1])
    print(result)

    # check if content of result.json == write_permission_list
    '''for i in range(len(result)):
        print("Result\tWrite_permission")
        print(result[i]+"\t"+write_permission_list[i]+"\n")
        if result[i] != write_permission_list[i]:
            print("No s'ha escrit en el mateix ordre.\n")'''


