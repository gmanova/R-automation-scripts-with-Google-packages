
rm(list = ls())
setwd("C:/Users/guyman/Documents")
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_191') 
Sys.setenv(TZ="GMT") # for lubridate's error log


# capture messages and errors to a file.
# logfile <- file("all.Rout", open="wt")
# sink(logfile, type="message")


if (!require('mailR')) install.packages('mailR')
if (!require('lubridate')) install.packages('lubridate')
if (!require('googledrive')) install.packages('googledrive')
library(lubridate)
library(googledrive)
library(mailR)



modifyTime <- today()-2
CanDelete <- c("name contains 'uploadkyc'",
             "name contains 'uploadisrael'",
             "name contains 'uploadchina'",
             "name contains 'uploadukraine'",
             "name contains 'uploadescalated'",
             "name contains 'ukrainetocy'",
             "name contains 'UploadUS'",
             "name contains 'chinatocy'",
             "name contains 'USTocy'",
             "name contains 'FreshPull'",
             "name contains 'UploadArchiveNew'",
             "name contains 'israeltocy'"
              # "name contains 'uploadarchive'"
            )

i <- 1
for (i in 1:length(CanDelete)){
  q1 <- CanDelete[i]
  q2 <- paste("modifiedTime < ", "'",modifyTime,"'", sep = "")
  delete <- drive_find(q = q1,
                        q = q2)
  drive_trash(delete)
  i <- i + 1
}

send.mail(from = "donotreply@etoro.com",
          to = c("guyman@etoro.com"),
          subject = "google drive cleanup finished",
          body = "google drive cleanup finished",
          smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
          authenticate = FALSE,
          send = TRUE)
  
