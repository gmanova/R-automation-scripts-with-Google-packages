
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
if (!require('googledrive')) install.packages('dplyr')
library(dplyr)
library(lubridate)
library(googledrive)
library(mailR)



zz <- file("all.Rout", open="wt")
sink(zz, type="message")


## reset message sink and close the file connection



try(dir <- file.info(dir("C:/Users/guyman/Documents", full.names = TRUE, ignore.case = TRUE)))
try(rownames(dir))
try(dir$filename <- rownames(dir))
try(ForDeletion <- dir$ctime <= date(Sys.time()-200000) )
try(ForDeletionList <- dir[ForDeletion,8] )

try(CanDelete <- c("UploadIsrael", "UploadKYC" ,"UploadUkraine", "UploadChina"))


i <- 1
try(
for (i in 1:length(CanDelete)){
  ForDeletionPart <- ForDeletionList[grepl(paste(CanDelete[i]), ForDeletionList)]
  file.remove(ForDeletionPart)
  i <- i+1
}
)

try(
send.mail(from = "donotreply@etoro.com",
          to = c("guyman@etoro.com"),
          subject = "tabdev local cleanup finished",
          body = "tabdev local cleanup finished",
          smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
          authenticate = FALSE,
          send = TRUE)
)


sink(type="message")
close(zz)

results <- as.character(grep("error", readLines("all.Rout"), ignore.case = TRUE))


if (length(results)==0) {
  print("ok")
} else {
  send.mail(from = "donotreply@etoro.com",
            to = c("guyman@etoro.com"),
            subject = "There was a problem in Tabdev Local Cleanup",
            body = "bla",
            attach.files = paste("./", "all.Rout", sep = ""),         
            smtp = list(host.name = "smtp-relay.gmail.com", port = 25, ssl = FALSE),
            authenticate = FALSE,
            send = TRUE)
}

