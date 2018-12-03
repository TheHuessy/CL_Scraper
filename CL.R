library(httr)
library(bitops) 
library(RCurl)
library(rvest)
library(XML)
library(xml2)
library(jsonlite)
library(curl)
library(civis)

#####    CRAIGSLIST     #####

##Logs info
#options(timeout= 4000000)
tms <- data.frame(Date = as.character(),
                  Zip = as.character(),
                  Listings = as.character(),
                  Total_Time = as.character())


CL <- data.frame("Post_Title" = as.character(),
                 "Post_Date" = as.character(),
                 "Lat" = as.character(),
                 "Lon" = as.character(),
                 "Lat_Lon_Acc" = as.character(),
                 "Price" = as.character(),
                 "Beds" = as.character(),
                 "Baths" = as.character(),
                 "SQFT" = as.character(),
                 "Type" = as.character(),
                 "Laundry" = as.character(),
                 "Heating" = as.character(),
                 "Cooling" = as.character(),
                 "Pets" = as.character(),
                 "Parking" = as.character(),
                 "Post_Text" = as.character(),
                 "Post_Date" = as.character(),
                 "Scrape_Date" = as.character(),
                 "Scrape_Zip" = as.character(),
                 "Link" = as.character()
)

tsc <- data.frame(zip = as.character(),
                  mrd = as.character()
                  )

##Load in timestamp check file to use as reference

TSR <- read_civis("sandbox.craigslist_timestampcheck", database="City of Boston")


##Search Parameters
#Base URL
c.burl <- "https://boston.craigslist.org/search/aap?"
#radius around zipcode
dis <- "search_distance=1"
#Zip Code vorwahl
zvor <- "&postal=" #then the zip code
zips <- {c("02118",
           "02119",
           "02120",
           "02130",
           "02134",
           "02135",
           "02445",
           "02446",
           "02447",
           "02467",
           "02108",
           "02114",
           "02115",
           "02116",
           "02215",
           "02128",
           "02129",
           "02150",
           "02151",
           "02152",
           "02124",
           "02126",
           "02131",
           "02132",
           "02136",
           "02109",
           "02110",
           "02111",
           "02113",
           "02121",
           "02122",
           "02124",
           "02125",
           "02127",
           "02210"
)}

slp <- sample(1:6, 1)
print(paste("Sleeping for", slp, "seconds at", Sys.time()))
Sys.sleep(slp)

print("STARTING UP")

for(i in 1:length(zips)){
  mti <- Sys.time()
  strt <- TSR$mrd[which(paste(0,TSR$zip, sep = "") == zips[i])] 
  strt <- as.POSIXct(strt)
  if (length(strt) == 0){
    strt <- as.POSIXlt(1541750400, origin="1970-01-01",tz="GMT")
  }

    c.links <- as.character()
    c.url1 <- paste(c.burl, dis, zvor,zips[i], sep = "")
  
  ####
  print("URL BUILT")
  ####
  
  slp <- sample(1:6, 1)
  print(paste("Sleeping for", slp, "seconds at", Sys.time()))
  Sys.sleep(slp)
  
  ####
  print("TRYCATCH STARTED")
  ####
  
  chka <-  tryCatch({
    read_html(c.url1)
  },
  error = function(e){e}
  )
  
  if(inherits(chka, "error")) {
    print("URL Broken")
    next
  }
  
  slp <- sample(1:6, 1)
  print(paste("Sleeping for", slp, "seconds at", Sys.time()))
  Sys.sleep(slp)
  
  
  ####
  print("ATTEMPTING REAL CONNECTION")
  ####
  
  tiv <- read_html(c.url1)
  
  
  ####
  print("REAL CONNECTION COMPLETED")
  ####
  
  
  max.listings <- tiv %>%
    html_nodes(".button.pagenum") %>%
    html_nodes(".totalcount") %>%
    html_text() %>%
    unique() %>%
    as.numeric()
  
  
  ####
  print("MAX LISTINGS DONE")
  ####
  
  
  ##Add logic that looks at max posts and 
  pglim <- max.listings/120
  if (pglim < 1){
    pglim <- 0
    pgs <- as.numeric()
    print("No more than 1 page")
  } else if (pglim < 3){
    if(round(pglim, digits = 0) > pglim){
      pglim <- round(pglim, digits = 0)
      pgs <- seq(from = 120, to = pglim*120, by = 120)
    } else {
      pglim <- as.integer(pglim)
      pgs <- seq(from = 120, to = pglim*120, by = 120)
    }
  } else {
    pglim <- 3
    pgs <- seq(from = 120, to = pglim*120, by = 120)
  }
  
  
  ####
  print("PAGE LIMITS DONE")
  ####
  
  
  
  
  ##Page 1 work
  l1 <- tiv %>%
    html_nodes(".rows") %>%
    html_nodes(".result-row")
  
  #find and remove reposted links
  rps <- grep("repost", l1)
  
 #Get the timestamps of when the postings were uploaded
      dts <- l1 %>% 
        html_nodes(".result-info") %>% 
        html_children() %>% 
        html_attr("datetime") %>% 
        na.omit() %>% 
        as.POSIXct()
      
      #Find posts that are newer than the last scrape
      old <- which(dts < strt)
      
      #If there aren't any that are newer, we skip it
      if (length(l1) == length(old)){
        print(paste("No new postings for zip", zips[i]))
        next
      }
 
  #looking for datetimes to limit length of pull
  
  ##If date time in results is younger than most recent date time pulled for that zip, then skip it
  ##Establish a table of newest post timestamp per zip code
  mrd <- l1 %>% 
    html_nodes(".result-info") %>% 
    html_children() %>% 
    html_attr("datetime") %>% 
    na.omit() %>% 
    as.POSIXct() %>% 
    max()
  
  rps <- c(rps, old) %>% 
    na.omit()
  
  l1l <- l1[-rps] %>%
    html_nodes("a") %>%
    html_attr("href") %>%
    unique() %>%
    .[-grep("#", .)]
  
  
  c.links <- l1l
  
  
  ####
  print("PAGE ONE LINKS CAUGHT")
  ####
  
  
  #See if we need to keep going
  if (length(pgs) != 0){
    for (j in 1:length(pgs)){
      c.url <- paste(c.burl, dis, zvor,zips[i], "&s=", pgs[j], sep = "")
      
      ####
      print("ADDITIONAL PAGE URL BUILT")
      ####
      
      slp <- sample(1:6, 1)
      print(paste("Sleeping for", slp, "seconds at", Sys.time()))
      Sys.sleep(slp)
      
      ####
      print("TRYCATCH FOR SECONDARY PAGE")
      ####
      
      chkale <-  tryCatch({
        read_html(c.url)
      },
      error = function(e){e}
      )
      if(inherits(chkale, "error")) {
        print(paste("URL Problem at page", j, "for zip", zips[i]))
        next
      }
      
      ####
      print("TRYCATCH SUCCEEDED")
      ####
      
      slp <- sample(4:8, 1)
      print(paste("Sleeping for", slp, "seconds at", Sys.time()))
      Sys.sleep(slp)
      
      lis <- read_html(c.url) %>%
        html_nodes(".rows") %>%
        html_nodes(".result-row")
      
      rpsp <- grep("repost", lis)
      
#Get the timestamps of when the postings were uploaded
      dts <- lis %>% 
        html_nodes(".result-info") %>% 
        html_children() %>% 
        html_attr("datetime") %>% 
        na.omit() %>% 
        as.POSIXct()
      
      #Find posts that are newer than the last scrape
      old <- which(dts < strt)
      
      #If there aren't any that are newer, we skip it
      if (length(lis) == length(old)){
        print(paste("No new postings for zip", zips[i]))
        next
      }
      
      rpsp <- c(rpsp, old) %>% 
        unique()
      
      ####
      print("SECONDARY PAGE LINKS CAUGHT")
      ####
      
      lis <- lis[-rpsp] %>%
        html_nodes("a") %>%
        html_attr("href") %>%
        unique() %>%
        .[-grep("#", .)]
      ####
      print("SECONDARY PAGE LINKS PARSED")
      ####
      
      c.links <- c(c.links, lis)
      print(paste("Finished Page", j, "of", length(pgs), "for zip", i, "of", length(zips)))
      
    } # Pages Loop END
    
  } # If there is more than one page END
  
  #Start scraping c.links
  c.links <- unique(c.links)
  ####
  print("SETTING NS TO 0")
  ####
  ns <- 0
  print("STARTING LINK SCRAPE")
for (k in c.links){
  
  print(paste("Starting Link", which(c.links == k), "of", length(c.links)))
  
  slp <- sample(4:8, 1)
  print(paste("Sleeping for", slp, "seconds at", Sys.time()))
  Sys.sleep(slp)
  
    ####
    print("TRYCATCH FOR C.LINK ATTEMPTED")
    ####
    
    chkal <-  tryCatch({
      read_html(k)
    },
    error = function(e){e}
    )
    
    if(inherits(chkal, "error")) {
      print(paste("URL Problem at", k))
      next
    }
    
    ####
    print("TRYCATCH FOR C.LINK SUCEEDED")
    ####
    
    slp <- sample(4:8, 1)
    print(paste("Sleeping for", slp, "seconds at", Sys.time()))
    Sys.sleep(slp)
    
    sc <- read_html(curl(k, handle = curl::new_handle("useragent" = "Mozilla/5.0")))    #html_text(sc)
    
    ####
    print("READING VARIABLE DATA")
    ####
    
    ##Title
    tit <- sc %>%
      html_nodes(".postingtitle") %>%
      html_nodes(".postingtitletext") %>%
      html_nodes("#titletextonly") %>%
      html_text()
    if (length(tit) == 0){
      print("Probably a bad link")
      tit <- NA
    }
    ##Lat
    lat <- sc %>%
      html_nodes(".mapbox") %>%
      html_nodes("#map") %>%
      html_attr("data-latitude")
    if (length(lat) == 0){
      lat <- NA
    }
    
    ##Lon
    lon <- sc %>%
      html_nodes(".mapbox") %>%
      html_nodes("#map") %>%
      html_attr("data-longitude")
    if (length(lon) == 0){
      lon <- NA
    }
    
    ##Lat Lon Accuracy
    llac <- sc %>%
      html_nodes(".mapbox") %>%
      html_nodes("#map") %>%
      html_attr("data-accuracy")
    if (length(llac) == 0){
      llac <- NA
    }
    
    ##Price
    pr <- sc %>%
      html_nodes(".postingtitle") %>%
      html_nodes(".postingtitletext") %>%
      html_nodes(".price") %>%
      html_text()
    if (length(pr) == 0){
      pr <- NA
    }
    
    ##Beds
    bds <- sc %>%
      html_nodes(".mapAndAttrs") %>%
      html_nodes(".attrgroup") %>%
      html_nodes(".shared-line-bubble") %>%
      html_text() %>%
      .[grep("BR",.)] %>%
      gsub("BR.*", "", .)
    if (length(bds) == 0){
      bds <- NA
    }
    
    
    ##Baths
    bths <- sc %>%
      html_nodes(".mapAndAttrs") %>%
      html_nodes(".attrgroup") %>%
      html_nodes(".shared-line-bubble") %>%
      html_text() %>%
      .[grep("Ba",.)] %>%
      gsub(".*/ ", "", .) %>%
      gsub("Ba", "", .)
    if (length(bths) == 0){
      bths <- NA
    }
    
    
    ##SQFT
    sqt <- sc %>%
      html_nodes(".mapAndAttrs") %>%
      html_nodes(".attrgroup") %>%
      html_nodes(".shared-line-bubble") %>%
      html_text() %>%
      .[grep("ft",.)] %>%
      gsub("ft2", "", .)
    if (length(sqt) == 0){
      sqt <- NA
    }
    
    ##Type
    sp <- sc %>%
      html_nodes(".mapAndAttrs") %>%
      html_nodes(".attrgroup")
    htwrd <- c("apartment",
               "condo",
               "cottage/cabin",
               "duplex",
               "flat",
               "house",
               "in-law",
               "loft",
               "townhouse",
               "manufactured",
               "assisted living"
    )
    ht <- sp %>%
      html_children() %>%
      html_text() %>%
      grep(paste(htwrd, collapse = "|"), ., value = TRUE)
    if (length(ht) == 0){
      ht <- NA
    }
    
    ##Laundry
    lwrd <- c("laundry", "w/d")
    lndy <- sp %>%
      html_children() %>%
      html_text() %>%
      grep(paste(lwrd, collapse = "|"), ., value = TRUE)
    if (length(lndy) == 0){
      lndy <- NA
    }
    
    ##Pets
    pts <- sp %>%
      html_children() %>%
      html_text() %>%
      grep(" OK", ., value = TRUE)
    if (length(pts) == 0){
      pts <- NA
    }
    
    
    ##Parking
    prkwd <- c("garage", "parking")
    prkg <- sp %>%
      html_children() %>%
      html_text() %>%
      grep(paste(prkwd, collapse = "|"), ., value = TRUE)
    if (length(prkg) == 0){
      prkg <- NA
    }
    
    ##Scrape_Date
    sdat <- Sys.time()
    if (length(sdat) == 0){
      sdat <- NA
    }
    
    
    ##Post Date
    pdt <- sc %>%
      html_nodes("#display-date") %>%
      html_nodes(".date.timeago") %>%
      html_attr("datetime")
    if (length(pdt) == 0){
      pdt <- NA
    }
    
    ##Post Body
    bod <- sc %>%
      html_nodes("#postingbody") %>%
      html_text() %>%
      gsub("\n", "  ", .)
    if (length(bod) == 0){
      bod <- NA
    }
    
    ##Posting Date
    pdt <- sc %>% 
      html_nodes(".dateReplyBar") %>% 
      html_nodes(".reply-button-row") %>% 
      html_nodes("#display-date") %>% 
      html_nodes(".date.timeago") %>% 
      html_attr("datetime")
    if (length(pdt) == 0){
      pdt <- NA
    }
    
    ####
    print("COMPILING DATA ROW")
    ####
    cgrp <- data.frame("Post_Title" = tit,
                       "Post_Date" = pdt,
                       "Lat" = lat,
                       "Lon" = lon,
                       "Lat_Lon_Acc" = llac, #I think that a value of 10 is an exact match, not sure about 22 or other values
                       "Price" = pr,
                       "Beds" = bds,
                       "Baths" = bths,
                       "SQFT" = sqt,
                       "Type" = ht,
                       "Laundry" = lndy,
                       "Heating" = NA,
                       "Cooling" = NA,
                       "Parking" = prkg,
                       "Post_Text" = bod,
                       "Post_Date" = pdt,
                       "Scrape_Date" = sdat,
                       "Scrape_Zip" = zips[i],
                       "Link" = k
    )
    
    if (nrow(cgrp) == 0){
      print(paste("No data for listing", which(c.links == k)))
      next
    }
    
    
    ####
    print("RBINDING DATA ROW")
    ####
    
    CL <- rbind(CL,cgrp)
    ns <- ns+1
    print("++++")
    print(paste("Finished Listing", which(c.links == k), "of", length(c.links), "for zip", i, "of", length(zips)))
    print("----")
    closeAllConnections()
  } #Link scrape for loop END
  
  ####
  print("CALCULATING TIME")
  ####
  
  mto <- Sys.time()
  zt <- mto-mti
  
  ####
  print("CREATING LOG")
  ####
  
  tl <- data.frame(Date = format(Sys.time(), "%m-%d-%Y"),
                    Zip = zips[i],
                    Listings = ns,
                    Total_Time = zt)
  tms <- rbind(tms,tl)
  
  slp <- sample(4:8, 1)
  print(paste("Finished zip", zips[i], "--", i, "of", length(zips)))
  print(paste("Sleeping for", slp, "seconds at", Sys.time()))
  Sys.sleep(slp)
  
  
  ##Timestamp log
  
  tsl <- data.frame(zip = as.character(zips[i]),
                    mrd = mrd)
  tsc <- rbind(tsl, tsc)
  
  slp <- sample(60:90, 1)
  print(paste("Super Sleeping for", slp, "seconds at", Sys.time()))
  Sys.sleep(slp)
  
}  #Zips for loop END

CL <- unique(CL)

pls <- nrow(CL)
lgs <- read_civis("sandbox.craigslist_logs", database="City of Boston")
lgs <- rbind(lgs, tms)



write_civis(tsc, tablename = "sandbox.craigslist_timestampcheck", if_exists = "append")




write_civis(lgs, tablename = "sandbox.craigslist_logs", if_exists = "append")


write_civis(CL, tablename = "sandbox.craigslist_master", if_exists = "append")

print(paste("Finished Scraping", pls, "new listings."))

