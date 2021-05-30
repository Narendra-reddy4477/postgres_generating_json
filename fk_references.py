# importing sqlparse module
import sqlparse

# defining function 'extract_definitions'.
def extract_definitions(token_list):
    # assumes that token_list is a parenthesis
    definitions = []
    tmp = []
    par_level = 0
    
    # iterating over 'token_list' for obtaining only constraints data
    for token in token_list.flatten():

        # filtering 'token' for punctuations for obtaining required fields
        if token.is_whitespace:
            continue
        elif token.match(sqlparse.tokens.Punctuation, '('):
            par_level += 1
            continue
        if token.match(sqlparse.tokens.Punctuation, ')'):
            if par_level == 0:
                break
            else:
                par_level += 1
        elif token.match(sqlparse.tokens.Punctuation, ','):
            if tmp:
                definitions.append(tmp)
            tmp = []
        else:
            tmp.append(token)
    if tmp:
        definitions.append(tmp)

    # returns list of objects which contains details that is related to each schema table
    return definitions


def fk_extracter(SQL):

    """
    Returns:
        list of dictionaries containing table names, column names
    """

    parsed = sqlparse.parse(SQL)[0]

    # extract the parenthesis which holds column definitions
    _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)

    columns = extract_definitions(par)

    # creating an empty list 'name_fk'
    name_fk = []    
    for column in columns:

        table_info = ' '.join(str(t) for t in column[1:])

        if table_info.__contains__('REFERENCES'):
            # creating an variable 'FK_labels' which contains list of strings('table_name', 'column_name')
            FK_labels=["table_name","column_name"]
            # using the split function to split by 'REFERENCES'
            table_info1 = table_info.split("REFERENCES")
            # obtaining table name and column name
            strip2 = table_info1[1].strip().split(' ')
            tableName = strip2[2]
            columnName = strip2[3]
            
            # using the zip function to bind tableName, columnName and assigning to the variable 'fk_ref_dict' in the form of dictionary
            fk_ref_dict = dict(zip(FK_labels,(tableName, columnName)))
            # appending 'fk_ref_dict' to the list 'name_fk'
            name_fk.append(fk_ref_dict)
    # returns the list of dictionaries containing the table names, column names
    return name_fk  