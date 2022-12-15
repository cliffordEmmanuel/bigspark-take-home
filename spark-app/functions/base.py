# from base import batch_params #, data_schema



import os

BATCH_DATA_SOURCE ='/home/clifford/side/projects/bigspark/data/tpcds_data_5g_batch'
DATA_SOURCE = '/home/clifford/side/projects/bigspark/data'

# ============================
# setting up the db configs





# ==========================================
# these functions are for dynamically selecting the datasets


def _parse_contents(batches:list, data_source=BATCH_DATA_SOURCE)->dict:
    data_paths={}
    for batch in batches:
        files=[]
        # gets all the subfolders in each batch    
        datasets=os.listdir(os.path.join(data_source,batch))
        for dataset in datasets:
            dataset_path=os.path.join(data_source,batch,dataset)
            csv_path= _return_csv_path(dataset_path)
            files.append((dataset,csv_path))

        data_paths[batch]=files

    return data_paths


def get_data_info(batch:str,meta_data):
    files =[]
    for data in meta_data[batch]:
        files.append(data)
    return files


# getting all the folders

def _get_batches_folders(data_source):
    batches=[]
    for dirs in os.listdir(data_source):
        if dirs.startswith('batch'):
            batches.append(dirs)
    
    batches.sort()
    return batches

# returning the file paths
def _return_csv_path(path):
    for root,dirs,files in os.walk(path):
        for f in files:
            if f.startswith('part'):
                return os.path.join(root,f)


# ==================================


