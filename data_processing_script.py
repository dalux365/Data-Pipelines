if __name__ == "__main__":

    # Get all file names in source data directory of companies whose data needs to be processed, 
    # This information is specified within the `top_companies.txt` file.
    
    import pandas as pd
    
    # Global path variables to use in "production" and preprod testing
    source_path = "/home/ec2-user/s3-drive/Stocks/"
    save_path = "/home/ec2-user/s3-drive/Output/"
    index_file_path = '/home/ec2-user/s3-drive/CompanyNames/top_companies.txt'
    processed_file_name = "historical_stock_data"
    header = True
    
            ######################## FUNCTION DEFINITIONS ########################
    
    # Function that returns a list of all the companies (represented by their csv files)
    # selected for inclusion within the data processing pipeline.
    def extract_companies_from_index(index_file_path):
        """Generate a list of company files that need to be processed. 

        Args:
            index_file_path (str): path to index file

        Returns:
            list: Names of company names. 
        """
        company_file = open(index_file_path, "r")
        contents = company_file.read()
        contents = contents.replace("'","")
        contents_list = contents.split(",")
        cleaned_contents_list = [item.strip() for item in contents_list]
        company_file.close()
        return cleaned_contents_list
    
    
    #function that attaches the source directory to each company csv file selected for processing.
    def get_path_to_company_data(list_of_companies, source_data_path):
        """Creates a list of the paths to the company data
        that will be processed

        Args:
            list_of_companies (list): Extracted `.csv` file names of companies whose data needs to be processed.
            source_data_path (str): Path to where the company `.csv` files are stored. 

        Returns:
            [type]: [description]
        """
        path_to_company_data = []
        for file_name in list_of_companies:
            path_to_company_data.append(source_data_path + file_name)
        return path_to_company_data


    #function that takes as input an array of company names 
    #(formed from the top_companies.txt file, representing multiple .csv files), 
    #and output a single .csv file containing the combined collection of data from these files, 
    #represented as rows, along with two additional summary columns providing extra context 
    #for each company entry.
    
    def data_processing(file_paths, output_path):
        """Process and collate company csv file data for use within the data processing component of the formed data pipeline.  

        Args:
            file_paths (list[str]): A list of paths to the company csv files that need to be processed. 
            output_path (str): The path to save the resulting csv file to.
            file_name (srt): The name you want the file to be saved as.
        """

        # for loop to collect the data and create a dataframe
        combined_data_list = []
        x = 0
        for company in file_paths:
            try:
                df = combined_data_list.append(pd.read_csv(company))
                for i in combined_data_list:
                    name = file_paths[x].split('/')[-1]
                    name_list = name.split('.')
                    name_only = name_list[0]
                    combined_data_list[x]['company_name'] = name_only
                x+=1 
            except pd.errors.EmptyDataError:
                continue

        combined_dataframe = pd.concat(combined_data_list)


        #Now, rename the columns and drop the ones I don't need
        combined_dataframe.rename(columns = {'Date':'stock_date','Open':'open_value',
                                            'High':'high_value','Low':'low_value',
                                            'Close':'close_value','Volume':'volume_traded'},inplace=True)

        combined_dataframe.drop('OpenInt', axis='columns',inplace=True)


        #Time to perfom some aggregations on the data
        combined_dataframe['daily_percent_change'] = (
            (combined_dataframe['close_value'] - combined_dataframe['open_value'])*100)

        combined_dataframe['value_change'] = (combined_dataframe['close_value'] - combined_dataframe['open_value'])

        #convert the column datatypes to those I need
        combined_dataframe['stock_date'] = pd.to_datetime(combined_dataframe['stock_date'])
        combined_dataframe['company_name'] = combined_dataframe['company_name'].astype(str)
        
        
        
        #output the dataframe as a .csv file to a directory of your choosing
        combined_dataframe.to_csv(output_path + processed_file_name + ".csv", index=False, header=header)
        

 
    
    file_names = extract_companies_from_index(index_file_path)
    

    # Update the company file names to include path information. 
    path_to_company_data = get_path_to_company_data(file_names, source_path)

    # Process company data and create full data output
    data_processing(path_to_company_data,save_path)
