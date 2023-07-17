############to build a table of solocam.cfg arguments given to a glider over the course of a deployment#######################
#get the original solocam.cfg file from GCP (just the first one the glider was deployed with), and then get all the solocam.cfg files send to a glider during deployment from the "Archive" folder of the glider terminal on the SFMC
#ensure the original solocam.cgf file has a name format of yyyymmdd_solocam.cfg (date should be the date the glider was deployed)
#set your working directory to the folder that has all solocam.cfg files
setwd("C:/Users/jen.walsh/Desktop/glider-file-test")

#create a list of file names
listcfg<-list.files(pattern="*_solocam.cfg")

#read each line of the list as a separate character string
all.cfg<-lapply(listcfg,readLines)

#unlist the list so that individual strings can be selected
all.cfg<-unlist(all.cfg)

library(stringr)

#select strings that start with "$" 
cfg.args<-all.cfg[grep("^\\$,.*",all.cfg)]

#create a dataframe so that a column with date/time stamp can be added
cfg.args<-as.data.frame(cfg.args)

#then create a list of individual characters
cfg.args<-str_split(cfg.args,",")

#then unlist cfg.args
cfg.args<-unlist(cfg.args)

#get rid of unwanted characters
cfg.args<-str_remove_all(cfg.args,"[\"]")
cfg.args<-str_remove_all(cfg.args,"[c($)]")
cfg.args<-str_remove_all(cfg.args,"[ \n]")
cfg.args<-cfg.args[cfg.args!=""]

#create a matrix first to specify how many arguments per row, then create a dataframe from that
m<-matrix(cfg.args,nrow=length(listcfg),byrow=TRUE)
cfg.args<-as.data.frame(m)

#add back a "c" column (the "c" is removed with code above that removes other unwanted characters, but if we leave that step out the dataframe isn't created correctly)
library(tibble)
c<-rep("c",length(listcfg))
cfg.args<-add_column(cfg.args,c,.after="V25")

#add a column of date/time
#split the original file names to isolate the date/time string
x<-str_split(listcfg,"_")

#unlist the list so that individual strings can be selected
x<-unlist(x)

#just select the date/time strings
x<-x[c(TRUE,FALSE)]

#split date time settings
y<-str_split(x,"T")

#unlist y so that individual strings can be selected
y<-unlist(y)
y<-append(y,"000000",after=1) #have to add a time in for the original files given to the glider (not through the SFMC). If the file isn't given to the glider via the SFMC it won't have a timestamp in this format. 
y.date<-y[c(TRUE,FALSE)]
y.time<-y[c(FALSE,TRUE)]

#create vector of file type
y.file<-rep("solocam.cfg",length(listcfg))

#create final dataframe
cfg.args<-cbind(y.date,y.time,y.file,cfg.args)

#make row id into a column
cfg.args<-rowid_to_column(cfg.args)
#add column names
solocam.fields<-c("cfg-id","date","time","cfg-source","settings","resolution","awb-redgain","awb-bluegain","monochrome","exposure","auto-exp","iso","frame-rate","shutter-duration","file-manager","file-type","file-name","timestamp","storage-shutdown","uv-lamp-manager","uv-lamp-on","uv-lamp-pwm","uv-lamp-duration","uv-lamp-schedule","sys-param","warmup","flash-duration","none1","none2","timelapse","timelapse-interval","timelapse-images","lamp","lamp-pwm")
colnames(cfg.args)<-solocam.fields

#make table
write.csv(cfg.args,"solocam-cfg-timeline.csv",row.names=FALSE)