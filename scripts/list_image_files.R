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
### Function to get data frame with file names
list_image_files <- function(imagery.url, subdirs) {
  # imagery.url: Drive url of imagery folder
  # subdirs: TRUE if there are directory subfolders, 
  #   eg DIR0000 for the glidercam, and the code should lapply through them. 
  #   If FALSE, drive_ls will be run with recursive=TRUE on imagery.url
  # Returns data frame with names of glider files
  
  stopifnot(
    require(googledrive), 
    require(dplyr)
  )
  
  img.df <- if (subdirs) {
    dir.ls <- drive_ls(path = as_id(imagery.url)) %>% arrange(name)
    images.list <- lapply(dir.ls$id, function(i) {
      print(filter(dir.ls, id == i)$name)
      drive_ls(path = as_id(i))
    })
    bind_rows(images.list)
    
  } else {
    drive_ls(as_id(imagery.url), recursive = TRUE)
  }
  
  if (any(duplicates(img.df$name))) warning("Duplicate image names")
  img.df %>% select(img_file = name)
}


### Function to write CSV to proper place
write_image_csv <- function(
    img.df, deployment, cam.type, 
    data.path = "C:/SMW/Gliders_Moorings/Gliders/Glider-Data-gcp/", 
    project = 'SANDIEGO', yr = '2022',
) {
  
  stopifnot(cam.type %in% c("glidercam", "shadowgraph"))
  
  out.path <- file.path(data.path, project, yr, deployment, "sensors", cam.type)
  if (!dir.exists(out.path)) stop("Directory does not exist: ", out.path)
  write.csv(img.df, file = file.path(out.path, "images.csv"), row.names = FALSE)
}



#-------------------------------------------------------------------------------
# Using list_image_files() for deployments

# data.path <- "C:/SMW/Gliders_Moorings/Gliders/Glider-Data-gcp/"
# sd.path <- "SANDIEGO/2022"


### amlr03-20220308
write_image_csv(
  list_image_files(
    "https://drive.google.com/drive/u/0/folders/16alDdJj6xm0Vshj9CN8pGfbQ0tUnlbkU",
    subdirs = TRUE
  ), 
  'amlr03-20220308', 'glidercam'
)


### amlr03-20220425
write_image_csv(
  list_image_files(
    "https://drive.google.com/drive/u/0/folders/1z72Dm-2TQpiEC1EkgMCu3yL9azs2QEW_",
    subdirs = TRUE
  ), 
  'amlr03-20220425', 'glidercam'
)


### amlr08-20220504
write_image_csv(
  list_image_files(
    "https://drive.google.com/drive/u/0/folders/1NSWocdAzNhfPdvuOTUaxQf-N0XRT7zEr",
    subdirs = FALSE
  ), 
  'amlr08-20220504', 'shadowgraph'
)
