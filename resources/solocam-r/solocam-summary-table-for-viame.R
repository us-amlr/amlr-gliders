#set your working directory to the folder that has all sample87.ma, proglets.dat, solocam.cfg, and solocam.ini files
setwd("C:/Users/jen.walsh/Desktop/glider-file-test")

#load libraries
library(dplyr)
library(stringr)
library(sqldf)

#for each file type (.ma, .cfg, .ini, .dat), create a list of file names
listma<-list.files(pattern="*_sample87.ma")
listcfg<-list.files(pattern="*_solocam.cfg")
listini<-list.files(pattern="*_solocam.ini")
listprog<-list.files(pattern="*_proglets.dat")

#create a dataframe for each file type that includes a column of the date/time file was given to glider, and the file commands
library(dplyr)

all.ma <- lapply(listma, function(x) {
  x.lines <- readLines(x)
  data.frame(
    file_name = x, 
    file_lines = x.lines
  )
})

all.ma.df <- bind_rows(all.ma)

all.cfg <- lapply(listcfg, function(x) {
  x.lines <- readLines(x)
  data.frame(
    file_name = x, 
    file_lines = x.lines
  )
})

all.cfg.df <- bind_rows(all.cfg)

all.ini <- lapply(listini, function(x) {
  x.lines <- readLines(x)
  data.frame(
    file_name = x, 
    file_lines = x.lines
  )
})

all.ini.df <- bind_rows(all.ini)

all.prog <- lapply(listprog, function(x) {
  x.lines <- readLines(x)
  data.frame(
    file_name = x, 
    file_lines = x.lines
  )
})

all.prog.df <- bind_rows(all.prog)

##############ma files######################################################
#get rid of all rows that don't contain arguments
#first, get rid of white space in columns
all.ma.df$file_lines<-str_squish(all.ma.df$file_lines)

#select only lines that begin with "b_arg"
all.ma.df<-all.ma.df%>%filter(str_detect(file_lines,"^b_arg:*"))

#to get rid of stuff behind the "#", split the character string
x<-str_split(all.ma.df$file_lines,"#")

#unlist to get rid of blank lines and stuff that isn't a b_arg
x<-unlist(x)
x<-x[x!=""]
x<-x[grep("^b_arg:*",x)]

#add x back as the file_names column
all.ma.df$file_lines<-x
############################################################################

##############cfg files#####################################################
#select only lines that begin with "$"
all.cfg.df<-all.cfg.df%>%filter(str_detect(file_lines,"^\\$,.*"))
############################################################################

##############ini files#####################################################
#select only lines that have a : and an _
all.ini.df<-all.ini.df%>%filter(str_detect(file_lines,":"))
all.ini.df<-all.ini.df%>%filter(str_detect(file_lines,"_"))
############################################################################

##############proglets files################################################
#in a separate object, select lines that begin with "#"
x<-all.prog.df%>%filter(str_detect(string=file_lines,pattern="#"))

#select lines in all.prog.df that are NOT INCLUDED in x
#sqldf doesn't like dataframe names with periods, so assign all.prog.df to z
z<-all.prog.df
all.prog.df<-sqldf("SELECT * FROM z EXCEPT SELECT * FROM x")

all.prog.df<-all.prog.df%>%filter(str_detect(string=file_lines,pattern="proglet"))

#get rid of empty lines
all.prog.df<-all.prog.df[all.prog.df$file_lines!="",]
############################################################################

#make table with rows from all files
all.args<-rbind(all.ma.df,all.cfg.df,all.ini.df,all.prog.df)
all.args<-as.data.frame(all.args)
write.csv(all.args,"solocam-timeline-amlrxx-ddmmyyyy.csv")

