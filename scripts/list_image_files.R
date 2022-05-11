# ls through Drive folder to create CSV with imagery file names


#-------------------------------------------------------------------------------
# ### Experimenting
# library(googledrive)
# library(dplyr)
# 
# imagery.url <- "https://drive.google.com/drive/u/0/folders/1z72Dm-2TQpiEC1EkgMCu3yL9azs2QEW_"
# x <- drive_ls(path = as_id(imagery.url)) %>% arrange(name)
# 
# # system.time(x.images <- drive_ls(path = as_id(imagery.url), recursive = TRUE))
# 
# 
# system.time({
#   x.list <- lapply(x$id, function(i) {
#     print(filter(x, id == i)$name)
#     drive_ls(path = as_id(i))
#   })
# })
# 
# x.df <- bind_rows(x.list) %>% select(name, id)



#-------------------------------------------------------------------------------
### Function
list_image_files <- function(imagery.url, recursive) {
  # imagery.url: Drive url of imagery folder
  # recursive: TRUE if there are direcotry subfolders, eg for the glidercam, 
  #   and the code should lapply through them. 
  #   If FALSE, drive_ls will be run with recursive=TRUE on imagery.url
  # Returns data frame with names of glider files
  
  stopifnot(
    require(googledrive), 
    require(dplyr)
  )
  
  img.df <- if (recursive) {
    dir.ls <- drive_ls(path = as_id(imagery.url)) %>% arrange(name)
    images.list <- lapply(dir.ls$id, function(i) {
      print(filter(dir.ls, id == i)$name)
      drive_ls(path = as_id(i))
    })
    bind_rows(images.list) %>% select(img_file = name)
    
  } else {
    drive_ls(as_id(imagery.url), recursive = TRUE)
  }
  
  img.df %>% select(img_file = name)
}


list_image_files("https://drive.google.com/drive/u/0/folders/1NSWocdAzNhfPdvuOTUaxQf-N0XRT7zEr", recursive = FALSE)
