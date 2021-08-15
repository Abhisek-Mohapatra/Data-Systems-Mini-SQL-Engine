import sqlparse  # Last Updated : Jan 22 10:17 pm  # (Final) 
from functools import reduce
from collections import defaultdict
import sys
import re


table_name = ''
table_dict = {}
values = {}

inv_table_dict={}  # For columns as keys it maps to tables


def checkAggregateType(fun_name):
    aggr_list = re.sub(r'[\(\)]', ' ', fun_name).split()  # ['max','col1']
    if(len(aggr_list) == 2):
        return True

    return False


def showOutput(result,cname,tname,op):
    tlist=tname.split(',')
    outputLst=[]
    outputStr=''

    if(op=='*'):
        for name in tlist:
            columns=table_dict[name]

            for c in columns:
                s=name+'.'+c
                outputLst.append(s)

        outputStr=(',').join(outputLst)
        print(outputStr)
    
    else:

        for c in cname:
            if(checkAggregateType(c)):
                outputLst.append(c)
                continue


            for t in tlist:
                if c in table_dict[t]:
                    s=t+'.'+c
                    outputLst.append(s)
                    break

        outputStr=(',').join(outputLst)
        print(outputStr)
        

    for record in result:
        if record == '':
            continue
        print(record)

def readCrossProduct(t_name):
    result=[]
    fname = t_name+'.csv'
    f = open(fname, 'r')
    content = f.read()
    temp = content.split('\n')

    for i in temp:
        if i == '':
            continue
        result.append(i)
    
    #print("Reading...")
    
    #print(result)
    

    return result



def getAggrFun(fun_name):

    aggr_list = re.sub(r'[\(\)]', ' ', fun_name).split()  # ['max','col1']
    if(len(aggr_list) == 2):
        return aggr_list





def filterVc(resultVc):
    new_list = []

    for i in resultVc:
        if i == '':
            continue
        st_result = i.split(',')
        temp_result = []
        for item in st_result:
            item = item.replace('"', '')
            temp_result.append(item)

        strTemp = (',').join(temp_result)

        new_list.append(strTemp)
        strTemp = ''

    return new_list


def parseMetaData():

    temp = []

    fpath = 'metadata.txt'
    with open(fpath) as f:
        for line in f:
            if(line.strip() == '<begin_table>'):
                continue
            elif(line.strip() != '<end_table>'):
                temp.append(line.strip().lower())
            elif(line.strip() == '<end_table>'):
                table_name = temp[0].lower()   # Added lower() Jan 17
                table_dict[table_name] = []
                table_dict[table_name] = temp[1:]
                temp = []

    


def getValuesFromCsv(t_name):
    lst = t_name.split(',')

    for tb in lst:
        values[tb] = []
        fname = tb+'.csv'
        f = open(fname, 'r')
        content = f.read()
        values[tb] = content.split('\n')

        for i in values[tb]:
            if i == '':
                values[tb].remove(i)

        


def crossJoin(tab_name):
    
    
    resultVc=[]
    temp=[]
    newStr=''

    tab_lst=tab_name.split(',')
    if(len(tab_lst)==1):
        resultVc=readCrossProduct(tab_name)
    
    else:

        resultVc=readCrossProduct(tab_lst[0])
        new_lst=[]
        tab_lst=tab_lst[1:]

        
        for i in tab_lst:
            temp=readCrossProduct(i)
            for record in resultVc:
                for rec in temp:
                    newStr=record+','+rec
                    new_lst.append(newStr)
            resultVc=new_lst
            new_lst=[]
        
    

    return resultVc
    


def processColumns(colNames, tab_name, resultVc):

    indexLst = []
    flag = {}
    searchLst = []

    for name in tab_name.split(','):
        if name not in table_dict.keys():
            
            sys.exit("Table name to be searched is not present")
        flag[name] = 0
        lst = table_dict[name]
        if flag[name] != 1:
            for i in lst:
                indexLst.append(i)
            flag[name] = 1
        else:
            pass

    for name in colNames:
        if name in indexLst:
            searchLst.append(indexLst.index(name))
        else:
            sys.exit("Column: "+name +
                     " is not present in the tables getting cross-joined")

    return searchLst


def processAggregateFunction(aggr_fun, final_result):

    resultLst = []
    final_result = [int(num) for num in final_result]

    if(aggr_fun.lower() == 'sum'):
        val = (reduce(lambda x, y: x+y, final_result))
        resultLst.append(str(val))

    elif(aggr_fun.lower() == 'max'):
        val = (reduce(lambda x, y: max(x, y), final_result))
        resultLst.append(str(val))

    elif(aggr_fun.lower() == 'min'):
        val = (reduce(lambda x, y: min(x, y), final_result))
        resultLst.append(str(val))

    elif(aggr_fun.lower() == 'count'):
        val = len(final_result)
        resultLst.append(str(val))

    elif(aggr_fun.lower() == 'average' or aggr_fun.lower()=='avg'):
        val = (reduce(lambda x, y: x+y, final_result))
        av = val/len(final_result)
        resultLst.append(str(av))
    else:
        sys.exit('The aggregate function '+aggr_fun.lower()+' is not supported')

    return resultLst


def processWhere(resultVc, tab_name, whereSplit):  # To process Where clause

    indexLst = []  # It has all the column names for all the tables involved
    flag = {}
    for name in tab_name.split(','):
        if name not in table_dict.keys():
            
            sys.exit("Table name to be searched is not present")
        flag[name] = 0
        # Stores column names in the table name into lst
        lst = table_dict[name]
        if flag[name] != 1:
            for i in lst:
                indexLst.append(i)
            flag[name] = 1
        else:
            pass

    tempLst = []
    evalString = ''
    val = 0
    getResultVc = []

    #print(whereSplit)
    #print(indexLst)

    for record in resultVc:
        tempLst = record.split(',')
        evalString = ''

        for i in whereSplit:  # it has all the conditions after where clause in the form of list
            if(i.lower() == 'and' or i.lower() == 'or'):
                evalString += ' '+i.lower()+' '
            elif(i == '='):
                evalString += '=='
            elif(i == '<='):
                evalString += i
            elif(i == '>='):
                evalString += i
            elif(i == '<'):
                evalString += i
            elif(i == '>'):
                evalString += i
            elif(i in indexLst):
                ind = indexLst.index(i)
                val = int(tempLst[ind])
                evalString += str(eval('val'))
            elif(i[0]=='-' or i.isdigit()):
                evalString += i
            else:
                sys.exit('Invalid operation or operand or operator exists')

        if(eval(evalString)):
            getResultVc.append(record)

    return getResultVc


def processOrderBy(resultVc, tab_name, orderBySplit):

    '''
    if(len(orderBySplit) != 2):
        sys.exit('Invalid query.Incorrect parameters in order by clause')
    '''
    
    if(len(orderBySplit)==1):
        orderBySplit.append('asc')

    indexLst = []  # It has all the column names for all the tables involved # eg:['A','B','C']
    flag = {}
    for name in tab_name.split(','):
        if name not in table_dict.keys():
            sys.exit("Table name to be searched is not present")
        flag[name] = 0
        # Stores column names in the table name into lst
        lst = table_dict[name]
        if flag[name] != 1:
            for i in lst:
                indexLst.append(i)
            flag[name] = 1
        else:
            pass

    colToProcess = orderBySplit[0]
    orderOp = orderBySplit[1]


    colToProcess = colToProcess

    if(colToProcess not in indexLst):
        sys.exit('Column name is not present..')

    orderDc = {}
    orderDc = defaultdict(list)
    key = 0
    getResultVc = []

    if(orderOp.lower() == 'asc'):
        for record in resultVc:
            tempLst = record.split(',')
            ind = indexLst.index(colToProcess)  # Get the index number
            key = int(tempLst[ind])

            # orderDc[key]=[]
            orderDc[key].append(record)

        for k in sorted(orderDc.keys()):
            for item in orderDc[k]:
                getResultVc.append(item)

    elif(orderOp.lower() == 'desc'):
        for record in resultVc:
            tempLst = record.split(',')
            ind = indexLst.index(colToProcess)  # Get the index number
            key = int(tempLst[ind])
            orderDc[key].append(record)

        for k in sorted(orderDc.keys()):
            for item in orderDc[k]:
                getResultVc.append(item)

        getResultVc = getResultVc[::-1]

    return getResultVc


def processGroupBy(groupBySplit, tab_name, resultVc, groupColumnVc, colNames):

    # column over which group by is done # eg: 'A'
    colToGroup = groupBySplit[0]
    dc = {}
    t_name = colNames     # eg: ['col1','max(col2)','min(col3)']

    finalResultVc = []  # To store the final result

    indexLst = []  # It has all the column names for all the tables involved # eg:['A','B','C']
    flag = {}
    for name in tab_name.split(','):
        if name not in table_dict.keys():
            # print(name)  # To be removed afterwards
            sys.exit("Table name is not present")
        flag[name] = 0
        # Stores column names in the table name into lst
        lst = table_dict[name]
        if flag[name] != 1:
            for i in lst:
                indexLst.append(i)
            flag[name] = 1
        else:
            pass

    # stores index number of colToGroup ie index of 'A'
    index_colToGroup = indexLst.index(colToGroup)

    # groupColumnVc : ['1','3','7']  # unique values in colToGroup ir 'A',say

    for i in groupColumnVc:
        dc[i] = ''
        for j in t_name:
            if(j == colToGroup):
                dc[i] += i+','
                continue
            elif(checkAggregateType(j)):
                getAggrList = getAggrFun(j)  # eg:['max','col1']
                op_name = getAggrList[0]
                col_name = getAggrList[1]


                if(col_name=='*' and op_name.lower()!='count'):
                    sys.exit('* cannot be used with any other aggreggate function other than count.The function used here is:'+op_name)
                if(col_name=='*' and op_name.lower()=='count'):
                    col_name=colToGroup

                getColVc = []

                for record in resultVc:
                    tmp = record.split(',')
                    # index of cuurent column inside aggregate function
                    index_currCol = indexLst.index(col_name)

                    if(tmp[index_colToGroup] == i):
                        getColVc.append(tmp[index_currCol])

                

                # max(col2) is processed and the return type is vector
                resLst = processAggregateFunction(op_name, getColVc)
                result = str(resLst[0])

                dc[i] += result+','
            else:
                sys.exit(
                    'Invalid query.More than one column or incorrect format of aggregate functions are present')

        finalResultVc.append(dc[i][0:len(dc[i])-1])

    return finalResultVc


def processAggregateWithoutGroup(colNames,tab_name,resultVc):

    indexLst = []  # It has all the column names for all the tables involved # eg:['A','B','C']
    flag = {}
    for name in tab_name.split(','):
        if name not in table_dict.keys():
            # print(name)  # To be removed afterwards
            sys.exit("Table name is not present")
        flag[name] = 0
        # Stores column names in the table name into lst
        lst = table_dict[name]
        if flag[name] != 1:
            for i in lst:
                indexLst.append(i)
            flag[name] = 1
        else:
            pass
    
    getColVc=[]
    outputLst=[]
    
    for j in colNames: # it has all aggregates in colNames eg: max(col1), min(col2)

        getAggrList = getAggrFun(j)  # eg:['max','col1']
        op_name = getAggrList[0]
        col_name = getAggrList[1]

        if(col_name=='*' and op_name.lower()!='count'):
            sys.exit('Invalid query. * cannot be used with any other aggreggate function other than count.The function used here is:'+op_name)
        if(col_name=='*' and op_name.lower()=='count'):
            outputLst.append(str(len(resultVc)))
            continue

        for record in resultVc:
            tmp = record.split(',')
            # index of cuurent column inside aggregate function
            index_currCol = indexLst.index(col_name)
            getColVc.append(tmp[index_currCol])  # getColVc stores the records related to that column and sends it for processing
    

        # max(col2) is processed and the return type is vector
        resLst = processAggregateFunction(op_name, getColVc)
        result = str(resLst[0])
        outputLst.append(result)
        getColVc=[]
    
    outputStr=''
    outputStr=(',').join(outputLst)
    outputLst=[]
    outputLst.append(outputStr)
    
    return outputLst
    
    


def processQueryRemaining(inputSql,token,colNames,tab_name,tokenList,distinctFlag):

    startToken=token

    whereFlag = 0  # to check for where flag

    whereSplit = []  # List

    if(inputSql.lower().find('where') > -1):
        whereFlag = 1
        whereSplit = inputSql.split(';')[0]
        #whereSplit = whereSplit.lower().split('where')[1].strip()
        whereSplit = whereSplit.split('where')[1].strip()
        # eg: ['col1','=','1','and','col2','=','9']
        whereSplit = whereSplit.split()
        temp=[]
        for i in whereSplit:
            if(i.lower()!='order' and i.lower()!='group'):
                temp.append(i)
            else:
                whereSplit=temp
                break

    orderByFlag = 0
    orderBySplit = []

    if(inputSql.lower().find('order by') > -1):
        orderByFlag = 1
        orderBySplit = inputSql.split(';')[0]  # String
        orderBySplit = orderBySplit.split('order by')[1].strip()
        orderBySplit = orderBySplit.split()  # eg:['col1','ASC']

    groupByFlag = 0
    groupBySplit = []

    if(inputSql.lower().find('group by') > -1):
        groupByFlag = 1
        groupBySplit = inputSql.split(';')[0]  # String
        groupBySplit = groupBySplit.split('group by')[1].strip()
        groupBySplit = groupBySplit.split()  # eg:['col1'] # eg:['a']
        temp=[]
        for i in groupBySplit:
            if(i.lower()!='order'):
                temp.append(i)
            else:
                groupBySplit=temp
                break

    # Here Tokens is presented as list
    sqlTokens = sqlparse.parse(inputSql)[0].tokens

    parseSql = sqlparse.sql.IdentifierList(sqlTokens)  # It denotes an Identifier object

    for i in parseSql.get_identifiers():
        # Tokens without spaces are added to tokenList[]
        tokenList.append(str(i))
    

    aggrFlag = 0
    new_aggr_list = re.sub(r'[\(\)]', ' ', (',').join(colNames)).split()  # ['max','col1']
    #print(new_aggr_list)
    if(len(new_aggr_list)>1):
        aggrFlag = 1

    
    

    if(whereFlag==1 and groupByFlag==1):
        if(inputSql.find('where')>inputSql.find('group by')):
            sys.exit('Invalid query. where statement is present after group by statement')
    
    if(whereFlag==1 and orderByFlag==1):
        if(inputSql.find('where')>inputSql.find('order by')):
            sys.exit('Invalid query. where statement is present after order by statement')

    if(groupByFlag==1):

        if(len(groupBySplit[0].split(','))>1):
            sys.exit('Invalid query. More than one columns are present for grouping in group by clause')
    
    if(orderByFlag==1):

        if(len(orderBySplit[0].split(','))>1):
            sys.exit('Invalid query. More than one columns are present for ordering in order by clause')



    if(startToken == '*'):
        #print('In *')

        colNames=[]
        op='*'

        getValuesFromCsv(tab_name)

        resultVc = crossJoin(tab_name)

        resultVc = filterVc(resultVc)  # To eliminate "" cases in the csv file

        if(whereFlag == 1):  # To handle 'where' clause
            resultVc = processWhere(resultVc, tab_name, whereSplit)

        
        if(orderByFlag == 1):
            resultVc = processOrderBy(resultVc, tab_name, orderBySplit)
        

        if(groupByFlag==1):
            sys.exit('Invalid query. groupBy clause is not supported with * operation')

        
        if(distinctFlag==1):
            result = []
                # To remove duplicates
            [result.append(val) for val in resultVc if val not in result]
            resultVc=result


        showOutput(resultVc,colNames,tab_name,op)
    

    
    
    elif(len(new_aggr_list)>1 and groupByFlag==0): # Means aggregate function is surely present with no group by
        #print('In Aggr')

        for i in colNames:
            if(not checkAggregateType(i)):
                sys.exit('Invalid query.Column name '+i+' is present for queries without group by clause.Only aggregate functions are allowed')



        #colOutput=token
        op=''

        '''

        colNames = []
        aggr_fun = new_aggr_list[0]  # max --> string
        col_name = new_aggr_list[1]  # col1  --> string

        colNames.append(col_name)
        '''

        # tab_name = tokenList[3].split(';')[0]  # tab_name is string
        getValuesFromCsv(tab_name)
         # contains the final cross-joined vector
        resultVc = crossJoin(tab_name)

        resultVc = filterVc(resultVc)  # To eliminate "" cases in the csv file

        if(whereFlag == 1):  # To handle 'where' clause
            resultVc = processWhere(resultVc, tab_name, whereSplit)
        

        if(orderByFlag==1):
            #print('Hmm')
            resultVc = processOrderBy(resultVc, tab_name, orderBySplit)
            #print(resultVc)
        

        resultVc=processAggregateWithoutGroup(colNames,tab_name,resultVc)

        if(distinctFlag==1):
            tmp = []
                # To remove duplicates
            [tmp.append(val) for val in resultVc if val not in tmp]
            resultVc=tmp

        showOutput(resultVc,colNames,tab_name,op)
    
    
    
    
    elif(groupByFlag == 1):
        #print('In Group By..')

        
        op=''
        getValuesFromCsv(tab_name)

         # contains the final cross-joined vector
        resultVc = crossJoin(tab_name)

        resultVc = filterVc(resultVc)

        if(whereFlag == 1):  # To handle 'where' clause  # Added Jan 13
            resultVc = processWhere(resultVc, tab_name, whereSplit)
        
        if(groupByFlag==1 and orderByFlag==1):

            if(whereFlag==1 and inputSql.find('where') > inputSql.find('group by')):
                sys.exit('Invalid query. where clause occurs after group by clause..')
            if(inputSql.find('order by')<inputSql.find('group by')):
                sys.exit('Invalid syntax. order by clause occurs before group by clause in the input query')

            
            if(groupBySplit[0].lower()!=orderBySplit[0].lower()):
                sys.exit('Invalid syntax. group by column: '+groupBySplit[0]+' and order by column: '+orderBySplit[0]+' are not same.')
        

        if(orderByFlag==1):
            #print('Hmm')
            resultVc = processOrderBy(resultVc, tab_name, orderBySplit)
            #print(resultVc)

        if(groupByFlag == 1):
            '''

            if(groupBySplit[0] not in colNames):
                sys.exit('Invalid Query. Column in the group by clause: '+groupBySplit[0]+' is not present in the select query')
            '''

            for i in colNames:
                if(not checkAggregateType(i) and i !=groupBySplit[0]):
                    sys.exit('Invalid syntax. column to be processed: '+i+' is not equal to column present in the group by clause: '+groupBySplit[0])
            

            #print('Hi')
            #print(groupBySplit)

            ind_lst = processColumns(groupBySplit, tab_name, resultVc)
            groupColumnVc = []
            lst = []
            groupString = ''

                # print(ind_lst)

            for i in resultVc:
                tmp = i.split(',')
                for j in ind_lst:
                    lst.append(tmp[j])
                groupString = (',').join(lst)
                groupColumnVc.append(groupString)
                lst = []

                # print(groupColumnVc)

            result = []
                # To remove duplicates
            [result.append(val) for val in groupColumnVc if val not in result]

            groupColumnVc = result  # Now it contains only unique values

                # print(groupColumnVc)
            
            #print('Heya')

            resultVc = processGroupBy(groupBySplit, tab_name, resultVc, groupColumnVc, colNames)
            #print(resultVc)
            #print(orderByFlag)
        
        '''
        
        if(orderByFlag==1):
            #print('Hmm')
            resultVc = processOrderBy(resultVc, tab_name, orderBySplit)
            #print(resultVc)
        

        '''
        
        if(distinctFlag==1):
            result = []
                # To remove duplicates
            [result.append(val) for val in resultVc if val not in result]
            resultVc=result


        showOutput(resultVc,colNames,tab_name,op)  # To print output in separate function
    
    elif(len(new_aggr_list)==1):  # That is if no aggregate functions are present eg: Select A,B from table1;

        #print('In No aggregate area..')
        #print(whereSplit)



        
        op=''

        getValuesFromCsv(tab_name)

            # contains the final cross-joined vector
        resultVc = crossJoin(tab_name)

            # To eliminate "" cases in the csv file
        resultVc = filterVc(resultVc)
        

        if(whereFlag == 1):  # To handle 'where' clause  # Added Jan 13
            

            resultVc = processWhere(resultVc, tab_name, whereSplit)
            

            # We get the required indices of the column names to be fetched
        

        if(orderByFlag==1):
            resultVc = processOrderBy(resultVc, tab_name, orderBySplit)
        
        
        if(groupByFlag == 1):
            

            ind_lst = processColumns(groupBySplit, tab_name, resultVc)
            groupColumnVc = []
            lst = []
            groupString = ''

                # print(ind_lst)

            for i in resultVc:
                tmp = i.split(',')
                for j in ind_lst:
                    lst.append(tmp[j])
                groupString = (',').join(lst)
                groupColumnVc.append(groupString)
                lst = []

                # print(groupColumnVc)

            result = []
                # To remove duplicates
            [result.append(val) for val in groupColumnVc if val not in result]

            groupColumnVc = result  # Now it contains only unique values

                # print(groupColumnVc)

            resultVc = processGroupBy(groupBySplit, tab_name, resultVc, groupColumnVc, colNames)
            
        '''
        
        if(orderByFlag==1):
            resultVc = processOrderBy(resultVc, tab_name, orderBySplit)
        '''
        

        ind_lst = processColumns(colNames, tab_name, resultVc)

        temp_result = []
        final_result = []
        strTemp = ''

        for i in resultVc:
            temp_result = i.split(',')
            for j in ind_lst:
                strTemp += temp_result[j]+','

            final_result.append(strTemp[0:len(strTemp)-1])
            strTemp = ''
            
        resultVc=final_result

        if(distinctFlag==1):
            result = []
                # To remove duplicates
            [result.append(val) for val in resultVc if val not in result]
            resultVc=result

        showOutput(resultVc,colNames,tab_name,op)  # To print output in separate function
    
    else:
        sys.exit('Invalid query..')







def processQuery():

    parseMetaData()   # MetaData.txt file is parsed

    tokenList = []
    resultVc = []
    #inputSql = input()
    inputSql=sys.argv[1]
    inputSql=inputSql.strip()
    #print(inputSql)

    if(inputSql.find(';')==-1):
        sys.exit('Invalid syntax. ; is not present in the input query')
    elif(inputSql.count(';')>1):
        sys.exit('Invalid query. More than one ; is present in the input query')



    # Check for ; error handling


    lst=inputSql.split(';')[0]
    lst=lst.split()
    tm=[]

    for i in lst:
        if(i.lower()=='select' or i.lower()=='distinct' or i.lower()=='group' or i.lower()=='order' or i.lower()=='by' or i.lower()=='where' or i.lower()=='from' or i.lower()=='asc' or i.lower()=='desc' or i.lower()=='and' or i.lower()=='or'):
            tm.append(i.lower())
        else:
            tm.append(i)
    
    inputSql=(' ').join(tm)

    inputSql=inputSql.lower()


    distinctFlag=0


    # Here Tokens is presented as list
    sqlTokens = sqlparse.parse(inputSql)[0].tokens

    parseSql = sqlparse.sql.IdentifierList(sqlTokens)  # It denotes an Identifier object

    for i in parseSql.get_identifiers():
        # Tokens without spaces are added to tokenList[]
        tokenList.append(str(i))
    

    ''' # Extra Info
    tokenList[]:

    select
    *
    from
    A
    where id = 1;

    '''
    '''
    print(tokenList[1].lower())
    print(tokenList[1].lower()=='distinct')
    print(len(tokenList))
    '''

    
    if(tokenList[0].lower()!='select'):
        sys.exit('Invalid syntax. select command is not present at the start of the query..')
    
    elif('from' not in tokenList):
        sys.exit('Invalid syntax. from keyword is not present in the query')


    if(len(tokenList) < 4):
        sys.exit("Insufficient number of arguments are present")
    
    elif(tokenList[1].lower() == 'distinct'):

        tokenList[2]=tokenList[2].strip().replace(' ','')
        tokenList[4]=tokenList[4].strip().replace(' ','')

        colNames = tokenList[2].split(',')
        tab_name = tokenList[4].split(';')[0]  # tab_name is string)
        distinctFlag=1

        processQueryRemaining(inputSql,tokenList[1],colNames,tab_name,tokenList,distinctFlag)

    else:

        tokenList[1]=tokenList[1].strip().replace(' ','')
        tokenList[3]=tokenList[3].strip().replace(' ','')

        colNames = tokenList[1].split(',')
        tab_name = tokenList[3].split(';')[0]  # tab_name is string)

        processQueryRemaining(inputSql,tokenList[1],colNames,tab_name,tokenList,distinctFlag)




processQuery()



