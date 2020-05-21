import pywren_ibm_cloud as pywren
import os
import pickle
import time

bucket = "sd-python"

N_SLAVES = 10

def master(id, x, ibm_cos):
    write_permission_list = []
    
    # 1. monitor COS bucket each X seconds
    objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
    for obj in objects:
        if "p_write_" in obj:
            
    while ("p_write_" in obj['Key'] ):            

    # 2. List all "p_write_{id}" files
        obj2 = list()
        for obj in objects:
            if "p_write_" in obj:
                obj2.append(obj)

    # 3. Order objects by time of creation
        #ordenat = sorted(obj2['LastModified'])
    
    # 4. Pop first object of the list "p_write_{id}"
    # 5. Write empty "write_{id}" object into COS
    # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
    # 7. Monitor "result.json" object each X seconds until it is updated
    # 8. Delete from COS “write_{id}”
    # 8. Back to step 1 until no "p_write_{id}" objects in the bucket
        #time.sleep(x)
        objects = ibm_cos.list_objects(Bucket=bucket)['Contents']
    
    return write_permission_list


def slave(id, x, ibm_cos):
    # 1. Write empty "p_write_{id}" object into COS
    f = open("p_write_"+str(id), "w")
    fSer = pickle.dumps(f)
    ibm_cos.put_object(Bucket=bucket, Key="p_write_"+str(id), Body=fSer)

    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    objects = ibm_cos.list_objects(Bucket=bucket)['Contents']['Key']
    while ("write_"+str(id) not in objects):
        objects = ibm_cos.list_objects(Bucket=bucket)['Contents']['Key']
        time.sleep(x)
    
    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    resultSer = ibm_cos.get_object(Bucket=bucket, Key='result.txt')['Body'].read()
    result = pickle.loads(resultSer)
    result.append(str(id))
    resultSer = pickle.dumps(result)
    ibm_cos.put_object(Bucket=bucket, Key="result.txt", Body=resultSer)

    # 4. Finish
    # No need to return anything


if __name__ == '__main__':

    pw = pywren.ibm_cf_executor()
    pw.call_async(master, 0)
    #pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()
    
    print(write_permission_list)
    # Get result.txt
    #result = ibm_cos.get_object(Bucket=bucket, Key='result.txt')['Body'].read()
    # check if content of result.txt == write_permission_list