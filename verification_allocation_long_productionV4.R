rm(list = ls())
setwd("C:/Users/guyman/Documents")
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_191') 
Sys.setenv(TZ="GMT") # for lubridate's error log

if (!require('dplyr')) install.packages('dplyr')
if (!require('RODBC')) install.packages('RODBC')
if (!require('googlesheets')) install.packages('googlesheets')
if (!require('mailR')) install.packages('mailR')
if (!require('lubridate')) install.packages('lubridate')
if (!require('googledrive')) install.packages('googledrive')
if (!require('sqldf')) install.packages('sqldf')
if (!require('tidyr')) install.packages('tidyr')

library(RODBC)
library(googlesheets)
library(dplyr)
library(mailR)
library(lubridate)
library(googledrive)
library(sqldf)
library(tidyr)

zz <- file("Errors_Long_Run.Rout", open="wt")
sink(zz, type="message")


start <- Sys.time()

try(send.mail(from = "donotreply@etoro.com",
          to = c("guyman@etoro.com"),
          subject = "Verification Allocation Long Run Is Starting",
          body = "Verification Allocation Long Run Is Starting",
          smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
          authenticate = FALSE,
          send = TRUE)
)



## uploading all the freshpull sheets as are to gdrive for reconceliation if needed

#returns list and browser authenticated once

try(my_sheets <- gs_ls() )


#########################################################################?????????????????????
### gdrive <- drive_find() #finds list of gdrive objects for the folder IDs
##########################################################################################


try(GSUkraine <-  gs_title("Verification_Allocation_Ukraine"))
try(FreshpullUkraine <- gs_read(ss = GSUkraine, ws = "FreshPull"))
Sys.sleep(30)
try(GSChina <-  gs_title("Verification_Allocation_China"))
try(FreshpullChina <- gs_read(ss = GSChina, ws = "FreshPull"))
Sys.sleep(30)
try(GSIsrael <-  gs_title("Verification_Allocation_Israel"))
try(FreshpullIsrael <- gs_read(ss = GSIsrael, ws = "FreshPull"))
Sys.sleep(30)
try(GSKYC <-  gs_title("Verification_Allocation_KYC"))
try(FreshpullKYC <- gs_read(ss = GSKYC, ws = "FreshPull"))
Sys.sleep(30)
try(GSEscalatedToCY <-  gs_title("Verification_Allocation_KYC"))
try(FreshpullEscalatedCy <- gs_read(ss = GSEscalatedToCY, ws = "EscalatedToCY"))
try(GSUS <-  gs_title("Verification_Allocation_US"))
try(FreshpullUS <- gs_read(ss = GSUS, ws = "FreshPull"))
Sys.sleep(30)

FilenameFreshpullUS <- paste("FreshpullUS ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(FreshpullUS, file = FilenameFreshpullUS)
try(drive_upload(FilenameFreshpullUS,path = as_id("1do97npG1GqRgflRbXa9ZYyFUhosEWQ3T")))

Sys.sleep(30)

FilenameFreshpullUkraine <- paste("FreshpullUkraine ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(FreshpullUkraine, file = FilenameFreshpullUkraine)
try(drive_upload(FilenameFreshpullUkraine,path = as_id("1dOa5L3kkYNNAx7-5UUr-d10Y-mQtZypk")))

Sys.sleep(30)

FilenameFreshpullKYC <- paste("FreshpullKYC ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(FreshpullKYC, file = FilenameFreshpullKYC)
try(drive_upload(FilenameFreshpullKYC,path = as_id("1dOa5L3kkYNNAx7-5UUr-d10Y-mQtZypk")))

Sys.sleep(30)

FilenameFreshpullChina <- paste("FreshpullChina ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(FreshpullChina, file = FilenameFreshpullChina)
try(drive_upload(FilenameFreshpullChina,path = as_id("1dOa5L3kkYNNAx7-5UUr-d10Y-mQtZypk")))

Sys.sleep(30)

FilenameFreshpullIsrael <- paste("FreshpullIsrael ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(FreshpullIsrael, file = FilenameFreshpullIsrael)
try(drive_upload(FilenameFreshpullIsrael,path = as_id("1dOa5L3kkYNNAx7-5UUr-d10Y-mQtZypk")))

Sys.sleep(30)

FilenameFreshpullEscalatedCy <- paste("FreshpullEscalatedCy ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(FreshpullEscalatedCy, file = FilenameFreshpullEscalatedCy)
try(drive_upload(FilenameFreshpullEscalatedCy,path = as_id("1dOa5L3kkYNNAx7-5UUr-d10Y-mQtZypk")))

Sys.sleep(30)

try(con <- odbcConnect("dwh_01_BI_DB",  uid = "guyman",  rows_at_time = 1))

try(d <- sqlQuery(con, paste("select * from BI_DB_VerificationsAllocations order by Country, IsDepositor desc"))) #query results object

try(odbcClose(con))

##########################################################################
################ UKRAINE ####################################################
#########################################################################

# subset Ukraine data from d

try(DataUkraine <- d %>% 
  filter(AllocateTo=='Ukraine') %>% 
  select(PullDate, Country, Region, PlayerStatus, RiskStatus, IsDepositor,UploadedBoth,VerificationLevelID,
         Priority,AllocateTo,TotalScore,RealCID) %>% 
  mutate(AssignedTo = "", Outcome = "", Comments = "") %>% 
  mutate(PullDate = as_date(PullDate))
)

write.csv(DataUkraine, "dataukraine.csv")

# create a dummy table to work around the lack of "delete from sheet" function in the library

dummy <- data.frame(matrix("", nrow = 10000, ncol = 15))
colnames(dummy) <- c('PullDate', 'Country', 'Region', 'PlayerStatus', 'RiskStatus', 'IsDepositor','UploadedBoth','VerificationLevelID',
                     'Priority','AllocateTo','TotalScore', 'AssignedTo', 'Outcome', 'Comments','RealCID')

# used this once just to populate some data 
# gs_edit_cells(ss = GSUkraine, ws = "FreshPull", input = DataUkraine,  trim = TRUE, col_names = TRUE)

# create the different subsets of data

try(nrowsUkraine <- nrow(gs_read(ss = GSUkraine, ws = "FreshPull")))

try(
if (class(gs_read(ss = GSUkraine, ws = "FreshPull")$PullDate)=="Date") {
  ExistingUkraine <- gs_read(ss = GSUkraine, ws = "FreshPull")%>% 
    filter(!is.na(RealCID))%>% 
    arrange(PullDate,desc(IsDepositor), Country) %>% 
    filter(RealCID != 1111111)  # all existing records in the sheet
} else {
  ExistingUkraine <- gs_read(ss = GSUkraine, ws = "FreshPull")%>% 
    filter(!is.na(RealCID))%>% 
    arrange(PullDate,desc(IsDepositor), Country) %>% 
    filter(RealCID != 1111111) %>% 
    mutate(PullDate = mdy(PullDate)) 
}
)

try(UnprocessedUkraine <- ExistingUkraine %>% # all the unprocessed items
  filter(is.na(Outcome)) %>% 
  filter(!is.na(RealCID)) 
)

try(ArchiveUkraine <- ExistingUkraine %>% # all the processed, going to archive
  filter(!is.na(Outcome)) %>%
  filter(!is.na(RealCID))
)

try(UkraineToCy <- ExistingUkraine %>% # all the Cyprus follow ups
  filter(Outcome == "FOLLOWUP CYP")%>%
  arrange(PullDate,desc(IsDepositor), Country) %>% 
  filter(!is.na(RealCID)) %>% 
  mutate(Outcome = "", AssignedTo = "")
)

# union the unprocessed data from the sheet with the new data pull and de-dupe CIDs

try(UploadUkraineNew <- sqldf("select * from DataUkraine where RealCID not in (select RealCID from ExistingUkraine)"))

try(UploadUkraineAll <- rbind(UnprocessedUkraine, UploadUkraineNew))

# dedupe

try(UploadUkraineAll <- UploadUkraineAll %>% 
  distinct(RealCID, .keep_all = TRUE) %>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

# prepare CSV's for manual uploads


FilenameUploadUkraineAll <- paste("UploadUkraineAll ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UploadUkraineAll, file = FilenameUploadUkraineAll)
drive_upload(FilenameUploadUkraineAll,path = as_id("1hNYQerM_vlSNU-b5l2bcd97d5S_t5bVn"))
Sys.sleep(5)
# 
FilenameUkraineToCy <- paste("UkraineToCy ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UkraineToCy, file = FilenameUkraineToCy)
drive_upload(FilenameUkraineToCy,path = as_id("1hNYQerM_vlSNU-b5l2bcd97d5S_t5bVn"))
Sys.sleep(5)

FilenameArchiveUkraine <- paste("ArchiveUkraine ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(ArchiveUkraine, file = FilenameArchiveUkraine)
drive_upload(FilenameArchiveUkraine,path = as_id("1hNYQerM_vlSNU-b5l2bcd97d5S_t5bVn"))
Sys.sleep(5)

FilenameUploadUkraineNew <- paste("UploadUkraineNew ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UploadUkraineNew, file = FilenameUploadUkraineNew)
drive_upload(FilenameUploadUkraineNew,path = as_id("1hNYQerM_vlSNU-b5l2bcd97d5S_t5bVn"))


Sys.sleep(30)

##########################################################################
################ China ####################################################
#########################################################################

# subset China data from d

try(DataChina <- d %>% 
  filter(AllocateTo=='China') %>% 
  select(PullDate, Country, Region, PlayerStatus, RiskStatus, IsDepositor,UploadedBoth,VerificationLevelID,
         Priority,AllocateTo,TotalScore,RealCID) %>%
  mutate(AssignedTo = "", Outcome = "", Comments = "") %>% 
  mutate(PullDate = as_date(PullDate))
)

write.csv(DataChina, "datachina.csv")

# create the different subsets of data

nrowsChina <- nrow(gs_read(ss = GSChina, ws = "FreshPull"))

try(
if (class(gs_read(ss = GSChina, ws = "FreshPull")$PullDate)=="Date") {
  ExistingChina <- gs_read(ss = GSChina, ws = "FreshPull")%>% 
    filter(!is.na(RealCID))%>% 
    arrange(PullDate,desc(IsDepositor), Country) %>% 
    filter(RealCID != 1111111)  # all existing records in the sheet
} else {
  ExistingChina <- gs_read(ss = GSChina, ws = "FreshPull")%>% 
    filter(!is.na(RealCID))%>% 
    arrange(PullDate,desc(IsDepositor), Country) %>% 
    filter(RealCID != 1111111) %>% 
    mutate(PullDate = mdy(PullDate)) 
}
)

try(UnprocessedChina <- ExistingChina %>% # all the unprocessed items
  filter(is.na(Outcome))%>% 
  filter(!is.na(RealCID))
)

try(ArchiveChina <- ExistingChina %>% # all the processed, going to archive
  filter(!is.na(Outcome))%>% 
  filter(!is.na(RealCID))
)

try(ChinaToCy <- ExistingChina %>% # all the Cyprus follow ups
  filter(Outcome == "FOLLOWUP CYP")%>% 
  filter(!is.na(RealCID))%>% 
  mutate(Outcome = "", AssignedTo = "")
)

# union the unprocessed data from the sheet with the new data pull and de-dupe CIDs

try(UploadChinaNew <- sqldf("select * from DataChina where RealCID not in (select RealCID from ExistingChina)"))

try(UploadChinaAll <- rbind(UnprocessedChina, UploadChinaNew)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

#dedupe

try(UploadChinaAll <- UploadChinaAll %>% 
  distinct(RealCID, .keep_all = TRUE)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

# prepare CSV's for manual uploads

FilenameUploadChinaAll <- paste("UploadChinaAll ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UploadChinaAll, file = FilenameUploadChinaAll)
drive_upload(FilenameUploadChinaAll,path = as_id("1LH8G56qQfBgyp_g7WxV2wnmJG43SAx_p"))
Sys.sleep(5)

FilenameChinaToCy <- paste("ChinaToCy ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(ChinaToCy, file = FilenameChinaToCy)
drive_upload(FilenameChinaToCy,path = as_id("1LH8G56qQfBgyp_g7WxV2wnmJG43SAx_p"))
Sys.sleep(5)

FilenameArchiveChina <- paste("ArchiveChina ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(ArchiveChina, file = FilenameArchiveChina)
drive_upload(FilenameArchiveChina,path = as_id("1LH8G56qQfBgyp_g7WxV2wnmJG43SAx_p"))


# move processed items to archive

# try(gs_add_row(ss = GSChina, ws = "Archive", input = ArchiveChina, verbose = TRUE))

Sys.sleep(30)

##########################################################################
################ Israel ####################################################
#########################################################################

# subset Israel data from d

try(DataIsrael <- d %>% 
  filter(AllocateTo=='Israel') %>% 
  select(PullDate, Country, Region, PlayerStatus, RiskStatus, IsDepositor,UploadedBoth,VerificationLevelID,
         Priority,AllocateTo,TotalScore,RealCID) %>%
  mutate(AssignedTo = "", Outcome = "", Comments = "") %>%
  mutate(PullDate = as_date(PullDate))
)

write.csv(DataIsrael, "dataisrael.csv")

# create the different subsets of data

try(nrowsIsrael<- nrow(gs_read(ss = GSIsrael, ws = "FreshPull")))

try(
if (class(gs_read(ss = GSIsrael, ws = "FreshPull")$PullDate)=="Date") {
  ExistingIsrael <- gs_read(ss = GSIsrael, ws = "FreshPull")%>% 
    filter(!is.na(RealCID))%>% 
    arrange(PullDate,desc(IsDepositor), Country) %>% 
    filter(RealCID != 1111111)  # all existing records in the sheet
} else {
  ExistingIsrael <- gs_read(ss = GSIsrael, ws = "FreshPull")%>% 
    filter(!is.na(RealCID))%>% 
    arrange(PullDate,desc(IsDepositor), Country) %>% 
    filter(RealCID != 1111111) %>% 
    mutate(PullDate = mdy(PullDate)) 
}
)

try(UnprocessedIsrael <- ExistingIsrael %>% # all the unprocessed items
  filter(is.na(Outcome)) %>% 
  filter(!is.na(RealCID))%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

try(ArchiveIsrael <- ExistingIsrael %>% # all the processed, going to archive
  filter(!is.na(Outcome))%>% 
  filter(!is.na(RealCID))%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

try(IsraelToCy <- ExistingIsrael %>% # all the Cyprus follow ups
  filter(Outcome == "FOLLOWUP CYP")%>% 
  filter(!is.na(RealCID))%>% 
  arrange(PullDate,desc(IsDepositor), Country)%>% 
  mutate(Outcome = "", AssignedTo = "")
)

# union the unprocessed data from the sheet with the new data pull and de-dupe CIDs

try(UploadIsraelNew <- sqldf("select * from DataIsrael where RealCID not in (select RealCID from ExistingIsrael)"))

try(UploadIsraelAll <- rbind(UnprocessedIsrael, UploadIsraelNew)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

#dedupe

try(UploadIsraelAll <- UploadIsraelAll %>% 
  distinct(RealCID, .keep_all = TRUE)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

# prepare CSV's for manual uploads

FilenameUploadIsraelAll <- paste("UploadIsraelAll ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UploadIsraelAll, file = FilenameUploadIsraelAll)
 drive_upload(FilenameUploadIsraelAll,path = as_id("1NIJmHbAh9nw5JZujAzvTsUVuH2T8C7YE"))
 Sys.sleep(5)
 
FilenameIsraelToCy <- paste("IsraelToCy ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(IsraelToCy, file = FilenameIsraelToCy)
 drive_upload(FilenameIsraelToCy,path = as_id("1NIJmHbAh9nw5JZujAzvTsUVuH2T8C7YE"))
 Sys.sleep(5)
 
FilenameArchiveIsrael <- paste("ArchiveIsrael ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(ArchiveIsrael, file = FilenameArchiveIsrael)
 drive_upload(FilenameArchiveIsrael,path = as_id("1NIJmHbAh9nw5JZujAzvTsUVuH2T8C7YE"))

# move processed items to archive

# try(gs_add_row(ss = GSIsrael, ws = "Archive", input = ArchiveIsrael, verbose = TRUE))

Sys.sleep(30)

##########################################################################
################ KYC ####################################################
#########################################################################

# subset KYC data from d

try(DataKYC <- d %>% 
  filter(grepl("KYC", d$AllocateTo)) %>% 
  select(PullDate, Country, Region, PlayerStatus, RiskStatus, IsDepositor,UploadedBoth,VerificationLevelID,
         Priority,AllocateTo,TotalScore,RealCID) %>%
  mutate(AssignedTo = "", Outcome = "", Comments = "") 
)

write.csv(DataKYC, "datakyc.csv")

# used this once just to populate some data 
# gs_edit_cells(ss = GSKYC, ws = "FreshPull", input = DataKYC,  trim = TRUE, col_names = TRUE)

# create the different subsets of data

try(nrowsKYC <- nrow(gs_read(ss = GSKYC, ws = "FreshPull")))

# googlesheets is giving some problems reading dates, so in if/else depending on the format: 

try(
if (class(gs_read(ss = GSKYC, ws = "FreshPull")$PullDate)=="Date") {
  ExistingKYC <- gs_read(ss = GSKYC, ws = "FreshPull")%>% 
    filter(!is.na(RealCID))%>% 
    arrange(PullDate,desc(IsDepositor), Country) %>% 
    filter(RealCID != 1111111)  # all existing records in the sheet
} else {
  ExistingKYC <- gs_read(ss = GSKYC, ws = "FreshPull")%>% 
    filter(!is.na(RealCID))%>% 
    arrange(PullDate,desc(IsDepositor), Country) %>% 
    filter(RealCID != 1111111) %>% 
    mutate(PullDate = mdy(PullDate)) 
}
)

try(UnprocessedKYC <- ExistingKYC %>% # all the unprocessed items
  filter(is.na(Outcome))%>% 
  filter(AllocateTo != 'KYC_Afterhours') %>%
  filter(!is.na(RealCID))
)

try(ArchiveKYC <- ExistingKYC %>% # all the processed, going to archive
  filter(!is.na(Outcome))%>% 
  filter(!is.na(RealCID))
)

try(UploadKYCNew <- sqldf("select * from DataKYC where RealCID not in (select RealCID from ExistingKYC)
                      and RealCID not in (select RealCID from ExistingChina) 
                      and RealCID not in (select RealCID from ExistingUkraine) 
                      and AllocateTo <> 'KYC_Afterhours'
                      order by Country, IsDepositor desc")
)

try(UploadKYCAll <- rbind(UnprocessedKYC, UploadKYCNew)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

write.csv(UploadKYCNew, "UploadKYCNew.csv")

#dedupe

UploadKYCAll <- UploadKYCAll %>% 
  distinct(RealCID, .keep_all = TRUE)%>% 
  filter(AllocateTo != 'KYC_Afterhours') %>%
  arrange(PullDate,desc(IsDepositor), Country)

# prepare CSV's for manual uploads

FilenameUploadKYCAll <- paste("UploadKYCAll ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UploadKYCAll, file = FilenameUploadKYCAll)
try(drive_upload(FilenameUploadKYCAll,path = as_id("1PMpxrXcLJpRmddwF8NdKrbBOKCN80S6h")))

FilenameArchiveKYC <- paste("ArchiveKYC ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(ArchiveKYC, file = FilenameArchiveKYC)
try(drive_upload(FilenameArchiveKYC,path = as_id("1PMpxrXcLJpRmddwF8NdKrbBOKCN80S6h")))

# move processed items to archive

# try(gs_add_row(ss = GSKYC, ws = "Archive", input = ArchiveKYC, verbose = TRUE))

Sys.sleep(30)


##########################################################################
################ USA ####################################################
#########################################################################

# subset US data from d

try(DataUS <- d %>% 
  filter(AllocateTo=='USA') %>% 
  select(PullDate, Country, Region, PlayerStatus, RiskStatus, IsDepositor,UploadedBoth,VerificationLevelID,
         Priority,AllocateTo,TotalScore,RealCID) %>%
  mutate(AssignedTo = "", Outcome = "", Comments = "") %>%
  mutate(PullDate = as_date(PullDate))
)

write.csv(DataUS, "dataUS.csv")

# create the different subsets of data

try(nrowsUS<- nrow(gs_read(ss = GSUS, ws = "FreshPull")))

try(
  if (class(gs_read(ss = GSUS, ws = "FreshPull")$PullDate)=="Date") {
    ExistingUS <- gs_read(ss = GSUS, ws = "FreshPull")%>% 
      filter(!is.na(RealCID))%>% 
      arrange(PullDate,desc(IsDepositor), Country) %>% 
      filter(RealCID != 1111111)  # all existing records in the sheet
  } else {
    ExistingUS <- gs_read(ss = GSUS, ws = "FreshPull")%>% 
      filter(!is.na(RealCID))%>% 
      arrange(PullDate,desc(IsDepositor), Country) %>% 
      filter(RealCID != 1111111) %>% 
      mutate(PullDate = mdy(PullDate)) 
  }
)

try(UnprocessedUS <- ExistingUS %>% # all the unprocessed items
  filter(is.na(Outcome)) %>% 
  filter(!is.na(RealCID))%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

try(ArchiveUS <- ExistingUS %>% # all the processed, going to archive
  filter(!is.na(Outcome))%>% 
  filter(!is.na(RealCID))%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

try(USToCy <- ExistingUS %>% # all the Cyprus follow ups
  filter(Outcome == "FOLLOWUP CYP")%>% 
  filter(!is.na(RealCID))%>% 
  arrange(PullDate,desc(IsDepositor), Country)%>% 
  mutate(Outcome = "", AssignedTo = "")
)
# union the unprocessed data from the sheet with the new data pull and de-dupe CIDs

try(UploadUSNew <- sqldf("select * from DataUS where RealCID not in (select RealCID from ExistingUS)"))

try(UploadUSAll <- rbind(UnprocessedUS, UploadUSNew)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

#dedupe

try(UploadUSAll <- UploadUSAll %>% 
  distinct(RealCID, .keep_all = TRUE)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)
# prepare CSV's for manual uploads

FilenameUploadUSAll <- paste("UploadUSAll ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UploadUSAll, file = FilenameUploadUSAll)
 drive_upload(FilenameUploadUSAll,path = as_id("1NIJmHbAh9nw5JZujAzvTsUVuH2T8C7YE"))
 Sys.sleep(5)
 
FilenameUSToCy <- paste("USToCy ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(USToCy, file = FilenameUSToCy)
 drive_upload(FilenameUSToCy,path = as_id("1NIJmHbAh9nw5JZujAzvTsUVuH2T8C7YE"))
 Sys.sleep(5)
 
FilenameArchiveUS <- paste("ArchiveUS ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(ArchiveUS, file = FilenameArchiveUS)
# drive_upload(FilenameArchiveUS,path = as_id("1NIJmHbAh9nw5JZujAzvTsUVuH2T8C7YE"))

# move processed items to archive

# try(gs_add_row(ss = GSUS, ws = "Archive", input = ArchiveUS, verbose = TRUE))

Sys.sleep(30)


##########################################################################
################ escalated to CY ####################################################
#########################################################################


# indicator for origin of escalation

UkraineToCy <- UkraineToCy %>% 
  mutate(AllocateTo = "UkraineToCy")

ChinaToCy <- ChinaToCy %>% 
  mutate(AllocateTo = "ChinaToCy")

IsraelToCy <- IsraelToCy %>% 
  mutate(AllocateTo = "IsraelToCy")

USlToCy <- USToCy %>% 
  mutate(AllocateTo = "USToCy")

# union as single dataset

try(DataEscalatedToCY <- rbind(UkraineToCy, ChinaToCy, IsraelToCy, USToCy) %>% 
  mutate(AssignedTo = AllocateTo) %>%
  mutate(PullDate = as_date(PullDate))%>% 
  arrange(AllocateTo,desc(IsDepositor), Country)
)
# create the different subsets of data

try(nrowsEscalatedToCY <- nrow(gs_read(ss = GSKYC, ws = "EscalatedToCY")))

try(
  if (class(gs_read(ss = GSKYC, ws = "FreshPull")$PullDate)=="Date") {
    ExistingEscalatedToCY <- gs_read(ss = GSKYC, ws = "EscalatedToCY")%>% 
      filter(!is.na(RealCID))%>% 
      arrange(PullDate,desc(IsDepositor), Country) %>% 
      filter(RealCID != 1111111)  # all existing records in the sheet
  } else {
    ExistingEscalatedToCY <- gs_read(ss = GSKYC, ws = "EscalatedToCY")%>% 
      filter(!is.na(RealCID))%>% 
      arrange(PullDate,desc(IsDepositor), Country) %>% 
      filter(RealCID != 1111111) %>% 
      mutate(PullDate = mdy(PullDate)) 
  }
)

try(UnprocessedEscalatedToCY <- ExistingEscalatedToCY %>% # all the unprocessed items
  filter(is.na(Outcome))
)

try(ArchiveEscalatedToCY <- ExistingEscalatedToCY %>% # all the processed, going to archive
  filter(!is.na(Outcome))
)

# union the unprocessed data from the sheet with the new data pull and de-dupe CIDs

try(UploadEscalatedToCYAll <- rbind(UnprocessedEscalatedToCY, DataEscalatedToCY)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

#dedupe

try(UploadEscalatedToCYAll <- UploadEscalatedToCYAll %>% 
  distinct(RealCID, .keep_all = TRUE)%>% 
  arrange(PullDate,desc(IsDepositor), Country)
)

try(UploadEscalatedToCYNew <- sqldf("select * from DataEscalatedToCY where RealCID not in (select RealCID from ExistingEscalatedToCY)"))


# prepare CSV's for manual uploads

FilenameUploadEscalatedToCYAll <- paste("UploadEscalatedToCYAll ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UploadEscalatedToCYAll, file = FilenameUploadEscalatedToCYAll)
 drive_upload(FilenameUploadEscalatedToCYAll,path = as_id("1PMpxrXcLJpRmddwF8NdKrbBOKCN80S6h"))
 Sys.sleep(5)
 
FilenameArchiveEscalatedToCY <- paste("ArchiveEscalatedToCY ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(ArchiveEscalatedToCY, file = FilenameArchiveEscalatedToCY)
 drive_upload(FilenameArchiveEscalatedToCY,path = as_id("1PMpxrXcLJpRmddwF8NdKrbBOKCN80S6h"))
 Sys.sleep(5)
 
FilenameUploadEscalatedToCYNew <- paste("UploadEscalatedToCYNew ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = "")
write.csv(UploadEscalatedToCYNew, file = FilenameUploadEscalatedToCYNew)
 drive_upload(FilenameUploadEscalatedToCYNew,path = as_id("1PMpxrXcLJpRmddwF8NdKrbBOKCN80S6h"))
 Sys.sleep(5)
 
# move processed items to archive

# try(gs_add_row(ss = GSEscalatedToCY, ws = "Archive", input = ArchiveEscalatedToCY, verbose = TRUE))

# writing the archived items into BI_DEV

try(ArchiveAll <- rbind(ArchiveUkraine, ArchiveChina, ArchiveKYC, ArchiveEscalatedToCY, ArchiveIsrael, ArchiveUS)%>% 
  mutate(OutcomeDate = Sys.Date()-1)
)

try(conDev <- odbcConnect("BI_DEV",  uid = "guyman",  rows_at_time = 1))

try(ArchiveAllExisting <- sqlQuery(conDev, paste("select * from Verifications_Allocations_Archive where
                                             OutcomeDate >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) 
                                             order by Country, IsDepositor desc")) # all from start of previous month
)

#coercing some data which is giving trouble writing back

try(ArchiveAllExisting$Country <- as.character(ArchiveAllExisting$Country))  
try(ArchiveAllExisting$RiskStatus <- as.character(ArchiveAllExisting$RiskStatus))
try(ArchiveAllExisting$AllocateTo <- as.character(ArchiveAllExisting$AllocateTo))
try(ArchiveAllExisting$Comments <- as.character(ArchiveAllExisting$Comments))

#dedupe and upload archive deltas

try(ArchiveAllNew <- rbind(ArchiveAllExisting,ArchiveAll) %>%
  distinct(Country,Region,PlayerStatus,RiskStatus,IsDepositor,UploadedBoth,VerificationLevelID,Priority,AllocateTo,
           TotalScore,RealCID,AssignedTo,Outcome,Comments,OutcomeDate, .keep_all = TRUE)
)

#coercing some data which is giving trouble writing back

try(ArchiveAllNew$Country <- as.character(ArchiveAllNew$Country) ) 
try(ArchiveAllNew$RiskStatus <- as.character(ArchiveAllNew$RiskStatus))
try(ArchiveAllNew$AllocateTo <- as.character(ArchiveAllNew$AllocateTo))
try(ArchiveAllNew$Comments <- as.character(ArchiveAllNew$Comments))

# create end of day unprocessed set

try(UnprocessedAll <- rbind(UnprocessedUkraine, UnprocessedKYC, UnprocessedChina, UnprocessedIsrael, UnprocessedEscalatedToCY, UnprocessedUS) %>% 
  mutate(OutcomeDate = date(Sys.time()), Outcome = "Unprocessed")
)

write.csv(UnprocessedAll, "UnprocessedAll.csv")


# create report for attachment

StatusReportDaily <- rbind(UnprocessedAll, ArchiveAllNew) %>% 
  mutate(Processed = ifelse(Outcome=='Unprocessed', 'Unprocessed', 'Processed'),
         Verified = ifelse(Outcome=='VERIFIED', 'Verified', 'NotVerified'),
         DayDiff = difftime(OutcomeDate,PullDate,units = ("days")),
         PullYear = year(PullDate), 
         PullMonth = month(PullDate),
         OutcomeYear = year(OutcomeDate), 
         OutcomeMonth = month(OutcomeDate)
  ) %>% 
  filter(OutcomeDate >= floor_date(Sys.Date()-month(1),"month")) %>%   # take only outcome date greater than 1st day of prev month
  distinct(Pulldate, Country, Region, PlayerStatus, RiskStatus, IsDepositor, UploadedBoth,
           VerificationLevelID, Priority, AllocationTo, TotalScore, RealCID, AssignedTo, Outcome, Comments, Processed, 
           Verified, .keep_all = TRUE)

StatusReportAggregated <- StatusReportDaily %>% 
  select(IsDepositor, AllocateTo, RealCID, PullYear, PullMonth, OutcomeYear, OutcomeMonth, Outcome, OutcomeDate) %>% 
  group_by(IsDepositor, AllocateTo, RealCID, Outcome, PullYear, PullMonth, OutcomeYear, OutcomeMonth) %>% 
  summarise(MaxDate = max(OutcomeDate)) %>% 
  spread(Outcome, MaxDate) %>% 
  mutate(Verified = ifelse(!is.na(VERIFIED), 'Verified', 'Not Verified')) %>% 
  select(-c(Unprocessed)) #get rid of the unprocessed as it's not a real status

ncloSRA <- ncol(StatusReportAggregated)

StatusReportAggregated$na_count <- apply(!is.na(StatusReportAggregated[8:ncol(StatusReportAggregated)-1]), 1, sum)-1

StatusReportAggregated <- StatusReportAggregated %>% 
  mutate(Processed = ifelse(na_count > 0, "Processed", "Not Processed")) %>% 
  select(-c(na_count)) %>% 
  mutate(Dummy1 = '', Dummy2 = '', Dummy3 = '',Dummy4='', Dummy5 = '')

ncolSRA <- ncol(StatusReportAggregated)

ReplaceNA <- StatusReportAggregated[,4:ncolSRA] <- sapply(StatusReportAggregated[,4:ncolSRA], as.character) # since your values are `factor`
ReplaceNA[is.na(ReplaceNA)] <- ""

StatusReportAggregated <- cbind(as.data.frame(StatusReportAggregated[1:3]), as.data.frame(ReplaceNA))

write.csv(StatusReportAggregated , "StatusReportAggregated.csv", row.names = FALSE)
write.csv(StatusReportDaily , "StatusReportDaily.csv", row.names = FALSE)

try(send.mail(from = "donotreply@etoro.com",
          to = c("guyman@etoro.com", "OPSVerificationAdmin@etoro.com", "UKRVerificationAdmin@etoro.com", "CNVerificationAdmin@etoro.com"),
          subject = "nightly reporting run finished (no writes)",
          html = TRUE,
          body = paste("Verification Allocation Nightly Run Finished, total time: ", difftime(Sys.time(), start, units = "mins"), "minutes", "<br>","<br>"),
          smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
          authenticate = FALSE,
          attach.files = c(paste("./", "StatusReportDaily.csv", sep = ""),
                           paste("./", "StatusReportAggregated.csv", sep = "")
          ),
          send = TRUE)
)

try(UploadArchiveNew <- anti_join(ArchiveAllNew,ArchiveAllExisting)) #returns only rows not joined

if(nrow(UploadArchiveNew) > 0){
try(sqlSave(conDev, UploadArchiveNew, "Verifications_Allocations_Archive", append = TRUE, rownames =  FALSE, 
            colnames = FALSE, fast = FALSE, verbose = TRUE))
}
  
# upload all archive items  to gdrive

try(FilenameUploadArchiveNew <- paste("UploadArchiveNew", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".csv", sep = ""))
try(write.csv(UploadArchiveNew, file = FilenameUploadArchiveNew, row.names = FALSE))
try(drive_upload(FilenameUploadArchiveNew,path = as_id("1wE2qzeXL6rT9lB_mW5s84cz5gO4kbMs_")))

# write unprocessed to database for archiving

if(nrow(UnprocessedAll) >0){
try(sqlSave(conDev, UnprocessedAll, "Verifications_Allocations_Unprocessed_Archive", append = TRUE, rownames =  FALSE, 
            colnames = FALSE, fast = FALSE, verbose = TRUE))
}

try(send.mail(from = "donotreply@etoro.com",
          to = c("guyman@etoro.com", "OPSVerificationAdmin@etoro.com"),
          subject = "nightly reporting run finished (with writes to BI_Dev)",
          html = TRUE,
          body = paste("Verification Allocation Nightly Run Finished, total time: ", difftime(Sys.time(), start, units = "mins"), "minutes", "<br>","<br>"),
          smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
          authenticate = FALSE,
          send = TRUE)
)

try(odbcClose(conDev))


# use the dummy empty table to delete the main sheet - Ukraine

i <- 2
j <- i + 20

while(j < nrowsUkraine){
  gs_edit_cells(ss = GSUkraine, ws = "FreshPull", input = dummy[i:j,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = j + 1
  j = i + 20
}

while(i <= nrowsUkraine+1){
  gs_edit_cells(ss = GSUkraine, ws = "FreshPull", input = dummy[1:1,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = i + 1
}


# write the data back to the main googlesheet

try(gs_edit_cells(ss = GSUkraine, ws = "FreshPull", input = UnprocessedUkraine, anchor = "A3", trim = FALSE, col_names = FALSE))
try(gs_edit_cells(ss = GSUkraine, ws = "FreshPull", input = UploadUkraineNew, anchor = paste("A", nrow(UnprocessedUkraine)+3,sep=""), trim = FALSE, col_names = FALSE))


# use dummy to delete from exisitng - China

i <- 2
j <- i + 20

while(j < nrowsChina){
  gs_edit_cells(ss = GSChina, ws = "FreshPull", input = dummy[i:j,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = j + 1
  j = i + 20
}

while(i <= nrowsChina+1){
  gs_edit_cells(ss = GSChina, ws = "FreshPull", input = dummy[1:1,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = i + 1
}


# write the data back to the main googlesheet

try(gs_edit_cells(ss = GSChina, ws = "FreshPull", input = UnprocessedChina, anchor = "A3", trim = FALSE, col_names = FALSE))

try(gs_edit_cells(ss = GSChina, ws = "FreshPull", input = UploadChinaNew, anchor = paste("A", nrow(UnprocessedChina)+3,sep=""), trim = FALSE, col_names = FALSE))

# move processed items to archive

# try(gs_add_row(ss = GSUkraine, ws = "Archive", input = ArchiveUkraine, verbose = TRUE))


# use dummy to delete from exisitng

i <- 2
j <- i + 20

while(j < nrowsIsrael){
  gs_edit_cells(ss = GSIsrael, ws = "FreshPull", input = dummy[i:j,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = j + 1
  j = i + 20
}
while(i <= nrowsIsrael+1){
  gs_edit_cells(ss = GSIsrael, ws = "FreshPull", input = dummy[1:1,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = i + 1
}

# write the data back to the main googlesheet

try(gs_edit_cells(ss = GSIsrael, ws = "FreshPull", input = UnprocessedIsrael, anchor = "A3", trim = FALSE, col_names = FALSE))

try(gs_edit_cells(ss = GSIsrael, ws = "FreshPull", input = UploadIsraelNew, anchor = paste("A", nrow(UnprocessedIsrael)+3,sep=""), trim = FALSE, col_names = FALSE))


# use dummy to delete from exisitng - KYC

i <- 2
j <- i + 20

while(j < nrowsKYC){
  gs_edit_cells(ss = GSKYC, ws = "FreshPull", input = dummy[i:j,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = j + 1
  j = i + 20
}
while(i <= nrowsKYC+1){
  gs_edit_cells(ss = GSKYC, ws = "FreshPull", input = dummy[1:1,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = i + 1
}

# union the unprocessed data from the sheet with the new data pull and de-dupe CIDs
#  write the data back to the main googlesheet

try(gs_edit_cells(ss = GSKYC, ws = "FreshPull", input = UnprocessedKYC, anchor = "A3", trim = FALSE, col_names = FALSE))

try(gs_edit_cells(ss = GSKYC, ws = "FreshPull", input = UploadKYCNew, anchor = paste("A", nrow(UnprocessedKYC)+3,sep=""), trim = FALSE, col_names = FALSE))


# use the dummy empty table to delete the main sheet - Escalated

i <- 2
j <- i + 20

while(j < nrowsEscalatedToCY){
  gs_edit_cells(ss = GSEscalatedToCY, ws = "EscalatedToCY", input = dummy[i:j,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = j + 1
  j = i + 20
}

while(i <= nrowsEscalatedToCY+1){
  gs_edit_cells(ss = GSEscalatedToCY, ws = "EscalatedToCY", input = dummy[1:1,], 
                anchor = paste("A",i+1, sep = "" ), trim = FALSE,verbose = TRUE, col_names = FALSE)
  i = i + 1
}


#  write the data back to the main googlesheet

try(gs_edit_cells(ss = GSKYC, ws = "EscalatedToCY", input = UnprocessedEscalatedToCY, anchor = "A3", trim = FALSE, col_names = FALSE))

try(gs_edit_cells(ss = GSKYC, ws = "EscalatedToCY", input = UploadEscalatedToCYNew, anchor = paste("A", nrow(UnprocessedEscalatedToCY)+3,sep=""), trim = FALSE, col_names = FALSE))


# end email

try(send.mail(from = "donotreply@etoro.com",
          to = c("guyman@etoro.com","OPSVerificationAdmin@etoro.com", "UKRVerificationAdmin@etoro.com", "CNVerificationAdmin@etoro.com"),
          subject = "Verification Allocation Nightly Run Finished (with writes)",
          html = TRUE,
          body = paste("Verification Allocation Nightly Run Finished, total time: ", difftime(Sys.time(), start, units = "mins"), "minutes", "<br>","<br>"),
          smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
          authenticate = FALSE,
          send = TRUE)
)

sink(type="message")
close(zz)

results <- as.character(grep("error", readLines("Errors_Long_Run.Rout"), ignore.case = TRUE))


# 
# if (length(results)==0) {
#   print("ok")
# } else {
#   send.mail(from = "donotreply@etoro.com",
#             to = c("guyman@etoro.com","staceyra@etoro.com","panayiotispa@etoro.com"),
#             subject = "There was a problem in nightly_run",
#             body = "looks like the nightly run did not run properly, attached error log.",
#             attach.files = paste("./", "Errors_Long_Run.Rout", sep = ""),         
#             smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
#             authenticate = FALSE,
#             send = TRUE)
# }


# "Ukraine Folder: https://drive.google.com/open?id=1hNYQerM_vlSNU-b5l2bcd97d5S_t5bVn", "<br>",
# "China Folder: https://drive.google.com/open?id=1LH8G56qQfBgyp_g7WxV2wnmJG43SAx_p","<br>",
# "Isreal Folder: https://drive.google.com/open?id=1LH8G56qQfBgyp_g7WxV2wnmJG43SAx_p","<br>",
# "KYC Folder: https://drive.google.com/drive/folders/1PMpxrXcLJpRmddwF8NdKrbBOKCN80S6h?usp=sharing","<br>","<br>",
# "Ukraine Googlesheet: https://docs.google.com/spreadsheets/d/1R-3PiEofWG81-WJmHDMhmRb2yKRl8xcMbNKKHUQD4qA/edit?usp=sharing","<br>",
# "China Googlesheet: https://docs.google.com/spreadsheets/d/1DWZEVVXg-3jGJEn4dXEFh4HqyfJCGwoYFlbF4kFKBwU/edit?usp=sharing", "<br>",
# "Israel Googlesheet: https://docs.google.com/spreadsheets/d/1hcHgvqwaurTcPzuQS7THe5TOdoM2qBjcF2GC6YqVZk0/edit?usp=sharing", "<br>",
# "KYC Googlesheet: https://docs.google.com/spreadsheets/d/1h-J9O5aUskaO0jnOaqBCKCF0a4hjUTiizkZT22teadQ/edit?usp=sharing"



# write to logfile and close connection

# sink(type="message")
# close(logfile)
# cat(readLines("all.Rout"),file=paste("logfile ", today("GMT")," ", hour(Sys.time()), " ",minute(Sys.time()),  ".txt", sep = ""),sep="\n")  

# write.csv(UnprocessedChina, "unprocessedchina.csv")
# write.csv(UnprocessedEscalatedToCY, "UnprocessedEscalatedToCY.csv")
# write.csv(UnprocessedIsrael, "UnprocessedIsrael.csv")
# write.csv(UnprocessedKYC, "UnprocessedKYC.csv")
# write.csv(UnprocessedUkraine, "UnprocessedUkraine.csv")

# freshpull: 1dOa5L3kkYNNAx7-5UUr-d10Y-mQtZypk
# archive to BI_DEV folder: 1wE2qzeXL6rT9lB_mW5s84cz5gO4kbMs_

