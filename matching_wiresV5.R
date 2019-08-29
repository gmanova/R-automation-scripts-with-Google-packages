rm(list = ls())
setwd("C:/Users/guyman/Documents/Rscripts/Matching CSVs") 
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_191') 
Sys.setenv(TZ="GMT") # for lubridate's error log

if (!require('dplyr')) install.packages('dplyr')
if (!require('RODBC')) install.packages('RODBC')
if (!require('mailR')) install.packages('mailR')
if (!require('sqldf')) install.packages('sqldf')
if (!require('tidyr')) install.packages('tidyr')
if (!require('lubridate')) install.packages('lubridate')
if (!require('RODBCext')) install.packages('RODBCext')
if (!require('openxlsx')) install.packages('openxlsx') 
if (!require('stringi')) install.packages('stringi') 
if (!require('googledrive')) install.packages('googledrive')
# if (!require('mgrittr')) install.packages('magrittr')

library(magrittr)
library(RODBC)
library(dplyr)
library(mailR)
library(sqldf)
library(tidyr)
library(lubridate)
library(RODBCext)
library(openxlsx)
library(stringi)
library(googledrive)

filename <- 
zz <- file("matchingErrors.Rout", open="wt")
sink(zz, type="message")


# access google drive and download the collecton files

q1 <- drive_find(pattern = "Wires_Cashout_Collection_V2.xlsx")
q2 <- drive_find(pattern = "Wires_Deposit_Collection_V2.xlsx")
COid <- q1[[2]]
DEPid <- q2[[2]]


# get modification time of the files from list item 3

CO_Current_Modify_Time <- as.POSIXlt(as.vector(unlist(q1[[3]])[20]), format =  "%Y-%m-%dT%H:%M", origin="1970-01-01")
Dep_Current_Modify_Time <- as.POSIXlt(as.vector(unlist(q2[[3]])[20]), format =  "%Y-%m-%dT%H:%M", origin="1970-01-01")

# CO_Current_Modify_Time <-  as.POSIXct(CO_Current_Modify_Time,origin="1970-01-01", format="%Y-%m-%dT%H:%M:%S", tz=Sys.timezone())
# Dep_Current_Modify_Time <- as.POSIXct(Dep_Current_Modify_Time,origin="1970-01-01", format="%Y-%m-%dT%H:%M:%S", tz=Sys.timezone())

# compare to previous modification time as saved on disc

PrevTimes <- read.csv("matching_modification_times.csv")


CO_Previous_Modify_Time <- as.POSIXlt(PrevTimes[1,2],origin="1970-01-01")
Dep_Previous_Modify_Time <-  as.POSIXlt(PrevTimes[2,2],origin="1970-01-01")


# if current modification time is later then previous then continue to run the process, else don't run

if (CO_Current_Modify_Time > CO_Previous_Modify_Time | Dep_Current_Modify_Time > Dep_Previous_Modify_Time) {


drive_download(as_id(COid), path = NULL, type = NULL, overwrite = TRUE,
               verbose = TRUE)
drive_download(as_id(DEPid), path = NULL, type = NULL, overwrite = TRUE,
               verbose = TRUE)


BO_deposits = read.xlsx("Wires_Deposit_Collection_V2.xlsx", sheet = "BO Load", colNames = TRUE)
Coutts_deposits = read.xlsx("Wires_Deposit_Collection_V2.xlsx", sheet = "coutts load", colNames = TRUE)
BO_cashouts = read.xlsx("Wires_Cashout_Collection_V2.xlsx", sheet = "BO Load", colNames = TRUE)
Coutts_cashouts = read.xlsx("Wires_Cashout_Collection_V2.xlsx", sheet = "Coutts Load", colNames = TRUE)
month_year = read.xlsx("Wires_Deposit_Collection_V2.xlsx", sheet = "month_and_year", colNames = TRUE)

# variables for sql stored procedures

match_month <- max(month_year$submit.match.month)
match_year <- max(month_year$submit.match.year)
# match_month <- 6
# match_year <- 2018

#fixing date import problems

BO_cashouts$Date <- convertToDate(BO_cashouts$Date)
BO_deposits$Date <- convertToDate(BO_deposits$Date)
Coutts_deposits$Date <- convertToDate(Coutts_deposits$Date)
Coutts_cashouts$Date <- convertToDate(Coutts_cashouts$Date)

#fix column names

Coutts_deposits$Source.Name <- NULL
Coutts_deposits$Year <- NULL
Coutts_deposits$Month <- NULL
Coutts_deposits$Day <- NULL
colnames(Coutts_deposits)[5] <- "TextAfterDelimiter1"
colnames(Coutts_deposits)[6] <- "TextAfterDelimiter2"
colnames(Coutts_deposits)[7] <- "TextAfterDelimiter3"
colnames(Coutts_deposits)[8] <- "TextAfterDelimiter4"

BO_deposits$Source.Name <- NULL
BO_deposits$Deposit.Time <- NULL
BO_deposits$Year <- NULL
BO_deposits$Month <- NULL
BO_deposits$Day <- NULL
colnames(BO_deposits)[1] <- "DepositStatus"
colnames(BO_deposits)[6] <- "PersonName1"
colnames(BO_deposits)[7] <- "PersonName2"
colnames(BO_deposits)[8] <- "PersonName3"
colnames(BO_deposits)[9] <- "PersonName4"
colnames(BO_deposits)[10] <- "PersonName5"

colnames(Coutts_cashouts)[1] <- "SourceName" 

conDev <- odbcConnect("BI_DEV",  uid = "guyman",  rows_at_time = 1)


# reorder to fit to sql server table and coerce to text

BO_deposits <- BO_deposits[c(12, 1:2, 11, 3:10)]
# BO_deposits$PersonName1 <- as.character(BO_deposits$PersonName1)
# BO_deposits$PersonName2 <- as.character(BO_deposits$PersonName2)
# BO_deposits$PersonName3 <- as.character(BO_deposits$PersonName3)
# BO_deposits$PersonName4 <- as.character(BO_deposits$PersonName4)
# BO_deposits$PersonName5 <- as.character(BO_deposits$PersonName5)
#  
# Coutts_deposits$TextAfterDelimiter1 <- as.character(Coutts_deposits$TextAfterDelimiter1)
# Coutts_deposits$TextAfterDelimiter2 <- as.character(Coutts_deposits$TextAfterDelimiter2)
# Coutts_deposits$TextAfterDelimiter3 <- as.character(Coutts_deposits$TextAfterDelimiter3)
# Coutts_deposits$TextAfterDelimiter4 <- as.character(Coutts_deposits$TextAfterDelimiter4)


try(sqlClear(conDev, "dbo.Matching_Wires_BO_Side"))
Sys.sleep(5)
try(sqlClear(conDev, "dbo.Matching_Wires_Coutts_Side"))
Sys.sleep(5)
try(sqlClear(conDev, "dbo.Matching_Deposits_BO_Side"))
Sys.sleep(5)
try(sqlClear(conDev, "dbo.Matching_Deposits_Coutts_Side"))
Sys.sleep(5)



try(sqlSave(conDev, BO_cashouts, "dbo.Matching_Wires_BO_Side", append = TRUE, rownames =  FALSE, 
            colnames = FALSE, fast = FALSE, verbose = TRUE))

try(sqlSave(conDev, Coutts_cashouts, "dbo.Matching_Wires_Coutts_Side", append = TRUE, rownames =  FALSE, 
            colnames = FALSE, fast = FALSE, verbose = TRUE))

try(sqlSave(conDev, BO_deposits, "dbo.Matching_Deposits_BO_Side", append = TRUE, rownames =  FALSE, 
            colnames = FALSE, fast = FALSE, verbose = TRUE))

try(sqlSave(conDev, Coutts_deposits, "dbo.Matching_Deposits_Coutts_Side", append = TRUE, rownames =  FALSE, 
            colnames = FALSE, fast = FALSE, verbose = TRUE))



# executing the stored procedures

sqlQuery(conDev, paste('exec [dbo].[SP_Matching_Wire_Deposits_ProviderToBO]', match_month,',', match_year, sep = ' '))
Sys.sleep(5)
sqlQuery(conDev, paste('exec [dbo].[SP_Matching_Wire_Deposits_BoToProvider]', match_month,',', match_year, sep = ' '))
Sys.sleep(5)
sqlQuery(conDev, paste('exec [dbo].[SP_Matching_Wire_Cashouts_BOToProvider]', match_month,',', match_year, sep = ' '))
Sys.sleep(5)
sqlQuery(conDev, paste('exec [dbo].[SP_Matching_Wire_Cashouts_ProviderToBO]', match_month,',', match_year, sep = ' '))


# this picks up the chinese names. similar solution for Russian has not worked

Sys.setlocale(category = "LC_CTYPE", locale = "Chinese")


Cashouts_Bo_To_Provider <- sqlQuery(conDev, 'select * from dbo.BI_DB_WireCashouts_Matched_BoToProvider order by Withdraw_Processing_ID, MatchType')
Cashouts_Provider_To_BO <- sqlQuery(conDev, 'select * from dbo.BI_DB_WireCashouts_Matched_ProviderToBO order by Description, Currency, Amount, MatchType')
Deposits_Bo_To_Provider <- sqlQuery(conDev, 'select * from dbo.BI_DB_WireDeposits_Matched_BoToProvider order by DepositID, MatchType')
Deposits_Provider_To_BO <- sqlQuery(conDev, 'select * from dbo.BI_DB_WireDeposits_Matched_ProviderToBO order by Description, Currency, Amount, MatchType')

# back to default system locale

Sys.setlocale(category = "LC_ALL", locale = "English_United States.1252")


write.csv(Cashouts_Bo_To_Provider, "Cashouts_Bo_To_Provider.csv", fileEncoding = "UTF-8")
write.csv(Cashouts_Provider_To_BO, "Cashouts_Provider_To_BO.csv", fileEncoding = "UTF-8")
write.csv(Deposits_Bo_To_Provider, "Deposits_Bo_To_Provider.csv", fileEncoding = "UTF-8")
write.csv(Deposits_Provider_To_BO, "Deposits_Provider_To_BO.csv", fileEncoding = "UTF-8")

send.mail(from = "donotreply@etoro.com",
          to = c("guyman@etoro.com", "annaan@etoro.com","theodorosky@etoro.com","anastasijast@etoro.com"),
          subject = paste("wire matching process finished for ", match_month, '/', match_year, sep = ''),
          html = TRUE,
          body = paste("wire matching process finished for ", match_month, '/', match_year, sep = ''),
          smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
          attach.files = c(paste("./", "Cashouts_Bo_To_Provider.csv", sep = ""),
                           paste("./", "Cashouts_Provider_To_BO.csv", sep = ""),
                           paste("./", "Deposits_Bo_To_Provider.csv", sep = ""),
                           paste("./", "Deposits_Provider_To_BO.csv", sep = "")
          ),
          authenticate = FALSE,
          send = TRUE)

odbcClose(conDev)

times <- rbind(as.character(CO_Current_Modify_Time), as.character(Dep_Current_Modify_Time))
write.csv (times, file = "matching_modification_times.csv")

}

# this just sends a notifier to me to show that the procedure isn't dead

send.mail(from = "donotreply@etoro.com",
          to = c("guyman@etoro.com"),
          subject = paste("wire matching process just ran"),
          html = TRUE,
          body = paste("wire matching process just ran"),
          smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
          authenticate = FALSE,
          send = TRUE)


# 
# ####################################### 
# # fix the non UTF names again
# #######################################
# 
# 
# cashoutNames <- sqlQuery(conDev, 'select RealCID, FirstName, LastName from DWH.dbo.Dim_Customer where RealCID in (select CID from dbo.Matching_Wires_BO_Side)')
# 
# cashoutNames <- cashoutNames %>% 
#   mutate(Fix_UTF = paste(FirstName, LastName, sep = ' ')) %>% 
#   select (-c(FirstName, LastName))
# 
# depositNames <- sqlQuery(conDev, 'select RealCID, FirstName, LastName from DWH.dbo.Dim_Customer where RealCID in (select CID from dbo.Matching_Deposits_BO_Side)')
# 
# depositNames <- depositNames %>% 
#   mutate(Fix_UTF = paste(FirstName, LastName, sep = ' ')) %>% 
#   select (-c(FirstName, LastName))
# 
# Cashouts_Bo_To_Provider <- sqldf('select a.*, b.Fix_UTF from Cashouts_Bo_To_Provider a join cashoutNames b on a.CID = b.RealCID')
# Deposits_Bo_To_Provider <- sqldf('select a.*, b.Fix_UTF from Deposits_Bo_To_Provider a join depositNames b on a.CID = b.RealCID')
# 
# ########################################
# # ########################################
# 
# # find UTF problems
# 
# Cashouts_Bo_To_Provider$concat <- paste(Cashouts_Bo_To_Provider$BOName1,Cashouts_Bo_To_Provider$BOName2,Cashouts_Bo_To_Provider$BOName3,
#                                         Cashouts_Bo_To_Provider$BOName4,Cashouts_Bo_To_Provider$BOName5, sep = '')
# # sub set with UTF problems
# 
# Cashouts_Bo_To_Provider[grepl("<U+", Cashouts_Bo_To_Provider$concat),]
# 
# # re-import with Russian encoding
# 
# 
# 
# Cashouts_Bo_To_Provider <- sqlQuery(conDev, 'select * from dbo.BI_DB_WireCashouts_Matched_BoToProvider order by Withdraw_Processing_ID, MatchType')
# 
