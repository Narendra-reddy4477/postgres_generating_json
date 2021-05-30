# importing required modules
import json
from ddl_parsing import DdlParse
import logging
import traceback
import fk_references

#configure the logging details   
logging.basicConfig(filename='log.txt',level=logging.INFO,format='%(asctime)s:%(levelname)s:%(message)s')

# defining function
def parsing(sample_ddl):
    """[parse the given schema and generates json which contains information about tables and related constraints]

    Args:
        sample_ddl ([type]): [string]
    """
    try:
        # obtaining table names and column names for multiple FOREIGN KEYS in the form of list of dictionaries
        fk_ref_details= fk_references.fk_extracter(sample_ddl)
        if len(fk_ref_details)==0:
            fk_ref_details=None
        else:
            pass
        # parsing of given sample_ddl schema
        parser = DdlParse(sample_ddl,None,fk_ref_details)
        table,schema_name= parser.parse()
        table.to_bigquery_fields()
        # creating an empty list 'final_array'
        final_array=[]
        # appending schema_name to the list 'final_array'
        final_array.append(schema_name)
        for col in table.columns.values():
            # creating an empty dictionary
            col_info = {}
            # obtaining the required fields and assigning the values to the related keys
            col_info["name"]                  = col.name
            col_info["data_type"]             = col.data_type
            col_info["length"]                = col.length
            col_info["precision(=length)"]    = col.precision
            col_info["scale"]                 = col.scale
            col_info["is_unsigned"]           = col.is_unsigned
            col_info["is_zerofill"]           = col.is_zerofill
            col_info["constraint"]            = col.constraint
            col_info["not_null"]              = col.not_null
            col_info["PK"]                    = col.primary_key
            col_info["FK"]                    = col.foreign_key
            col_info["unique"]                = col.unique
            col_info["auto_increment"]        = col.auto_increment
            col_info["distkey"]               = col.distkey
            col_info["sortkey"]               = col.sortkey
            col_info["encode"]                = col.encode
            col_info["default"]               = col.default
            col_info["character_set"]         = col.character_set
            col_info["bq_legacy_data_type"]   = col.bigquery_legacy_data_type
            col_info["bq_standard_data_type"] = col.bigquery_standard_data_type
            col_info["comment"]               = col.comment
            col_info["description(=comment)"] = col.description
            col_info["bigquery_field"]        = json.loads(col.to_bigquery_field())
 
            # appending the Key, Value pairs of 'col_info' dictionary to the list
            final_array.append(col_info)
        
        logging.info("json for  "+str(sample_ddl))
        logging.info(json.dumps(final_array, indent=2, ensure_ascii=False))
    except Exception :
        traceback.print_exc() 

# main function
if __name__=="__main__":
    
    try:
        # opening file 'firm.txt' and assigning to the variable 'file1'
        file1 = open("firm.txt", 'r')
        # opening the file 'firmschema.txt' in the write mode
        file2 = open("firmschema.txt", 'w')    

        # iterating each line in the 'file1'
        for line in file1.readlines():
            # filtering the unwanted lines
            if not line.endswith("postgres;\n"):
                
                if not (line.startswith("SET") or line.startswith("SELECT")):
                        
                        
                    if not (line.startswith("\.")):
                        
                        if not (line.startswith("COPY")):
                            
                            # writing obtained lines to the 'file2'
                            file2.write(line)
        file1.close()
        file2.close()

        # opening file 'firmschema.txt'
        file_dump=open("firmschema.txt")
        ddls=file_dump.read()
        file_dump.close()

        list_start_index=[]
        init_index=0
        index_=0


        # iterating the obtained schema to remove whitespaces and empty lines
        while True:
            index_=ddls.find("CREATE",init_index,-1)
            if index_==-1:
                break
            list_start_index.append(index_)
            init_index=index_+5


        #create a list of index for the character starting with ';'
        list_end_index=[]
        init_index=0
        index_=0
        while True:
            index_=ddls.find(";",init_index,-1)
            if index_==-1:
                break
            list_end_index.append(index_)
            init_index=index_+5
        #collecting all schema in dump as a list of string
        create_lines=[]
        for i in range(len(list_start_index)):
            create_lines.append(ddls[list_start_index[i]:list_end_index[i]])


        file_dump2=open("firmschema.txt")
        table1 = []
        
        for line in file_dump2.readlines():
            # obtaining table name, column name from the related constraints
            if line.startswith("ALTER"):
                table_name = line.split("ONLY")
                strip1 = table_name[1].strip("\n")
                table1.append(strip1)
            elif line.startswith("    ADD"):
                table1_info = line.split("CONSTRAINT")
                strip2 = table1_info[1].strip().split(' ')
                if(strip2[1]=='FOREIGN'):
                    table1.append(strip2[1] +' '+strip2[2]+' '+strip2[3]+' '+strip2[4]+' '+strip2[5])

                else:   
                    table1.append(strip2[1] +' '+strip2[2]+' '+strip2[3])
        print(table1)
        file_dump2.close()
        newlines = []

        # iterating over 'create_lines' list to insert the constraints into the related schema table
        for i in create_lines:
            newstr = ''
            flag = True
            for idx in range(len(table1)):
                if(i.__contains__(table1[idx]) and flag):        
                    newstr = i.replace('\n)',', '+table1[idx+1]+')')
                    newstr = newstr.replace(';','')
                    
                    newlines.append(newstr)            
                    flag = False

                elif (i.__contains__(table1[idx]) and (flag == False)):   
                    newlines[len(newlines)-1] = newlines[len(newlines)-1].replace('))','), ')

                    newstr = table1[idx+1]+')'
                    newstr = newstr.replace(';','')
                    newlines[len(newlines)-1] = newlines[len(newlines)-1]+newstr
        
        # passing 'newlines' to the function call 'parsing'.
        for line in newlines:
            parsing(line)

                
    except Exception as e:
        logging.info("Error while uploading file {}".format(e))
    
            
            



            