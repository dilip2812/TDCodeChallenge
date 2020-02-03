import datetime
import requests
import getpass
import urllib3

# define Python user-defined exceptions

class Error(Exception):
   """Base class for other exceptions"""
   pass

class FormatError(Error):
   """Raised when the input format is invalid"""
   pass
   
class LimitError(Error):
   """Raised when the limit value is breached """
   pass

# Define Constant
class CONST_DATA:

    DATAGOVURL = "https://api.data.gov.in/resource/"
    SUPPORTED_FORMAT = ['json','csv','xml']
    
    AIRINDEX = {
        "API_HASH" : "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69",
        "MAX_LIMIT" : 10000
        }
    
    API_KEY = "579b464db66ec23bdd0000011322f4f3c3c744386b31786f75d86802"
    
class LoadAirIndex:

    def __init__(self, user):
        self.user = user
        now = datetime.datetime.now()
        current_time = now.strftime("%H_%M_%S")
        log_name = user + "_" +current_time + ".log"
        self.log_name = log_name
    
    def downlaoddata(self,outtype,offset,limit):
        
        try:
            # Validate request
            if CONST_DATA.SUPPORTED_FORMAT.count(outtype) > 0 :
                pass
            else:
                raise FormatError()
            
            if limit > CONST_DATA.AIRINDEX["MAX_LIMIT"]:
                raise LimitError()
                
        except FormatError:
            print('Invalid Format! Changing to default')
            # set default 
			
            format='xml'
            
			try:
			
                if limit > CONST_DATA.AIRINDEX["MAX_LIMIT"]:
                    raise LimitError()
					
            except LimitError:
			
                print("Limit Breached! Changing to default")
                # set default 
                limit = 10
				
        except LimitError:
		
            print("Limit Breached! Changing to default")
            # set default 
            limit = 10
			
        finally:
		
            # disable InsecureRequest warning
            filename = self.log_name.split('.')[0] + "." + outtype
            finalurl = ''.join([CONST_DATA.DATAGOVURL,CONST_DATA.AIRINDEX["API_HASH"] ,"?api-key=",CONST_DATA.API_KEY,"&format=",outtype,"&offset=",str(offset),"&limit=",str(limit)])
            print("FinalURL is :",finalurl)
            f = open(filename,'w')
			
            try:
                Outfile = requests.get(finalurl, verify=False,stream=False)
				
            except:
			
                print("Error is ",sys.exc_info()[0])
                raise;
            
            print(Outfile.text,file=f)
            
def main():
    loadairind = LoadAirIndex(getpass.getuser())
    output_format = input("Enter output format " + str(CONST_DATA.SUPPORTED_FORMAT) + " ").lower()
    
    # print(output_format)
    try:
        input_limit = int(input("Enter input limit "))
    except:
        # defaulting to input 
        input_limit = 10
    
    loadairind.downlaoddata(output_format,0,input_limit)
    
if __name__ == "__main__":
    main()
	