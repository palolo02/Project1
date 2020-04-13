# ==================================================
# Function to clean data 
# Objective: Split the text that comes with the '||' format to separate the categories for each column
# Parameters:
#     df = DataFrame to use
#     indx = Main column to merge with the original DataFrame
#     col = Name of the colum to clean within the original DataFrame
#     txtRemove = Category we need to remove after cleansing process
#     pivotTable = Pivot table to deliver only one single index to merge with the original Dataset (if needed)
#     isNotNumber = Determine if the column to process does not contain numbers in its values (like age)
#     removeCommas = Get rid of , and blank spaces for coumns with dirty data
# ==================================================

print("Loading function...")

def splitColumnsDF(df, indx, col, txtRemove = "", pivotTable = False, isNotNumber=True, removeCommas=False):
    
    # Import dependencies
    import pandas
    
    print("======================================")
    # Identify DataFrame to clean all columns    
    tempDF = df[[indx,col,'year','state','city_or_county','n_killed','n_injured']]
    
    # Filter to get the desire years of analysis    
    tempDF = tempDF.loc[(tempDF["year"] >= 2014) & (tempDF["year"] <= 2017)]
    print(f"Starting analysis for {col}...")
    
    
    print(f"Removing empty values...")
    # Removing empty values
    tempDF.dropna(axis=1,how="all")
    tempDF = tempDF[tempDF[col].notna()]
        
    # Debugging
    #display(tempDF.head())
    
    # Replace , by | and apply the same cleaning process. We initialize Capital letter for each word i.e female = Female
    if(removeCommas == True):
        tempDF[col] = tempDF[col].str.replace(", ","|", regex=True)
        tempDF[col] = tempDF[col].str.title()
    
    # Create temporary dataframe to split rows by |
    internalTempDf = pandas.DataFrame(tempDF[col].str.split("|",expand = True))
    # reset index to allow duplicates in 'indx'. The split will cause duplicate rows for different values
    internalTempDf = internalTempDf.reset_index()
    
    print("Splitting dataframe...")
    # Debugging   
    #display(internalTempDf.head())
    
    # Creatting a transpose DataFrame to allow multiple rows for each splitted column
    internalTempDf = internalTempDf.melt(id_vars = ["index"], var_name = "Total", value_name="Value")
    # Set 1 for each row to sum up when aggregating data
    internalTempDf["Total"] = 1
    # Drop na values (for all the empty values. Reduce size)
    internalTempDf = internalTempDf.dropna(how="any")
    
    # Replace all the [#::] pattern to clean format. Fix to process the age
    if(isNotNumber):
        internalTempDf["Value"] = internalTempDf["Value"].str.replace("[0-9]*::?|[0-9]*","", regex=True)
    else:
        internalTempDf["Value"] = internalTempDf["Value"].str.replace("[0-9]*::?","", regex=True)
      
    # Debugging
    #display(internalTempDf.head(100))
    
    print("Grouping values...")
    # Group categories in the DataFrame to avoiod duplicates. Sum up the Total count perr category
    internalTempDf = internalTempDf.groupby(by=["index","Value"], as_index=False).sum()
    
    # Remove empty values (if needed) or 'Uknown' categoires in the data by accessing the index
    conditionRemove = internalTempDf.loc[(internalTempDf["Value"] == "") | 
                                         (internalTempDf["Value"] == txtRemove)].index
    # Drop the index rows from the dataframe
    internalTempDf.drop(conditionRemove, inplace=True)
    # Adjunst the index to match the original indx
    internalTempDf.set_index("index",inplace=True)
    if (col == "participant_age"):
        internalTempDf["Value"] = internalTempDf["Value"].astype("int")
    
    # Debugging
    #display(internalTempDf.head(100))
    print(f"Merging {indx}...")
    # Merge the original dataframe to acces the indx column
    internalTempDf = internalTempDf.merge(tempDF, how="left", left_index=True, right_index=True)
    internalTempDf.drop(columns=[col], inplace=True)
    
    # Debugging
    #display(internalTempDf.head())
    #display(internalTempDf["incident_id"].count())
    print("Completed!")
    
    # Return the cleaned dataframe 
    return internalTempDf
    
# ==================================================
# Function to clean data over a large dataset (chunck data in n steps)
# Objective: Read groups of a large dataset to clean columns and avoid memory errors
# Parameters:
#     url = Filename to read it
# ==================================================

def readWholeDataset(url):
    # Import dependencies
    import pandas
    import numpy
    import calendar
    
    # Get total number of rows
    total = 239679
    # iterations to perform over the large dataset
    steps = 7
    # Number of rows to read in each steps
    rowsSize = int(total/steps)
    # Number os rows read so far (to skip rows)
    rowsRead = 0
     
    # Repeat the same cleaning process each time with a new dataset
    for i in range(0,steps):
        # if this is the first group set, we initialize the dataframes 
        if(rowsRead == 0):
            #Debuggin
            print(f"Gun Stolen First iteration - {rowsSize} rows")
            # Read file
            gunViolenceDf = pandas.read_csv(url, nrows = rowsSize)
            
            # Add extra columns to handle data plots eventually
            gunViolenceDf['date'] = pandas.to_datetime(gunViolenceDf['date'])
            gunViolenceDf['year'] = gunViolenceDf['date'].dt.year
            gunViolenceDf['month'] = gunViolenceDf['date'].dt.month
            gunViolenceDf['monthday'] = gunViolenceDf['date'].dt.day
            gunViolenceDf['weekday'] = gunViolenceDf['date'].dt.weekday
            
            # Apply cleaning process and store dataframes
            gunStolenDF = splitColumnsDF(gunViolenceDf,"incident_id","gun_stolen", "Unknown", True)
            gunTypeDF = splitColumnsDF(gunViolenceDf,"incident_id","gun_type", "Unknown", isNotNumber=False)
            ageDF = splitColumnsDF(gunViolenceDf,"incident_id","participant_age", "Unknown", False, isNotNumber=False)
            ageGroupDF = splitColumnsDF(gunViolenceDf,"incident_id","participant_age_group", "Unknown", True)
            genderDF = splitColumnsDF(gunViolenceDf,"incident_id","participant_gender", "Unknown", removeCommas=True)
            relationshipDF = splitColumnsDF(gunViolenceDf,"incident_id","participant_relationship", "Unknown", False)
            statusDF = splitColumnsDF(gunViolenceDf,"incident_id","participant_status", "Unknown", False)
            typeDF = splitColumnsDF(gunViolenceDf,"incident_id","participant_type", "Unknown", True)
        
        # Read groups of data rows
        else:
            # Debugging
            print(f"******************************* {i+1} iteration - Rows: {rowsRead} | RowsSize: {rowsSize} ***************")
            # read the csv from 'n' row to 'm' row
            temp = pandas.read_csv(url, skiprows=[i for i in range(1,rowsRead)], nrows=rowsSize)
            
            # Add extra columns to handle data plots
            temp['date'] = pandas.to_datetime(temp['date'])
            temp['year'] = temp['date'].dt.year
            temp['month'] = temp['date'].dt.month
            temp['monthday'] = temp['date'].dt.day
            temp['weekday'] = temp['date'].dt.weekday
                       
            print("Appending dataframe...")
            #Debugging
            #display(temp.head())
            
            # Append the resulting dataframe to the original after cleaning the desire column
            # Here, we clean each group and append it to the first one
            gunViolenceDf = gunViolenceDf.append(temp, ignore_index=True)
            gunStolenDF = gunStolenDF.append(splitColumnsDF(temp,"incident_id","gun_stolen", "Unknown", True), ignore_index=True)
            gunTypeDF = gunTypeDF.append(splitColumnsDF(temp,"incident_id","gun_type", "Other", isNotNumber=False))
            ageDF = ageDF.append(splitColumnsDF(temp,"incident_id","participant_age", "Unknown", False, isNotNumber=False))
            ageGroupDF = ageGroupDF.append(splitColumnsDF(temp,"incident_id","participant_age_group", "Unknown", True))
            genderDF = genderDF.append(splitColumnsDF(temp,"incident_id","participant_gender", "Unknown", removeCommas=True))
            relationshipDF = relationshipDF.append(splitColumnsDF(temp,"incident_id","participant_relationship", "Unknown", False))
            statusDF = statusDF.append(splitColumnsDF(temp,"incident_id","participant_status", "Unknown", False))
            typeDF = typeDF.append(splitColumnsDF(temp,"incident_id","participant_type", "Unknown", True))
        
        # Update the rows we have read so far
        rowsRead += rowsSize
    
    
    # Remove the unused cloumns for the original dataframe
    try:
        gunViolenceDf.drop(columns=["address","incident_url","source_url","participant_type",
                         "congressional_district","state_house_district","participant_relationship",
                         "participant_age","location_description","incident_characteristics",
                         "notes","participant_age_group","participant_name","participant_status",
                         "state_senate_district","participant_gender","incident_url_fields_missing",
                         "sources","gun_stolen","gun_type"], inplace=True)
    except:
        print("Columns already deleted")
    
    # Export data to picle format for load faster
    print("Saving cleaned dataframes...")
    gunViolenceDf.to_pickle("Resources/gunViolence.pkl")
    gunStolenDF.to_pickle("Resources/gunStolen.pkl")
    gunTypeDF.to_pickle("Resources/gunType.pkl")
    ageDF.to_pickle("Resources/age.pkl")
    ageGroupDF.to_pickle("Resources/ageGroup.pkl")
    genderDF.to_pickle("Resources/gender.pkl")
    relationshipDF.to_pickle("Resources/relationship.pkl")
    statusDF.to_pickle("Resources/status.pkl")
    typeDF.to_pickle("Resources/type.pkl")
    
    # Debugging
    print("Finished!")    
    
    # Complete the function
    return 
    
