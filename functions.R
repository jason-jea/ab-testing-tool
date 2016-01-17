library(survival)
library(ggplot2)
library(plyr)
library(dplyr)
library(reshape2)
library(RODBC)
library(RJDBC)
library(RPostgreSQL)
library(tidyr)
library(lubridate)
library(rmarkdown)
library(scales)
library(RColorBrewer)
library(stringr)
library(httr)
library(RCurl)
library(httr)
library(readr)


## category, affinity
## gender, location, active status in app
## retarget: store page visit (how many days ago?), push campaign, email campaign
## segment (.edu?)
## a/b test: how many groups?

writeBin(
  aws.s3::getobject(
    bucket = 's3-rpt-uss-dat-warehouse',
    key = 'AKIAJFWGWAYSAVIYVUHA',
    secret = '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',
    object = "prd/bi/push_tool/v01/domains/truncate-loadall/domain_names000.gz"
  )$content,"domains.gz")

domains <-
  read.table(gzfile("domains.gz"))

# drv <- dbDriver("PostgreSQL")
# 
# redshift = dbConnect(drv, host = 'rsh-rpt-se1-dat-rdb-mem-prd.c2vtvr6b5gso.us-east-1.redshift.amazonaws.com', dbname = 'members',user = "rmn_jjea", password = "182493Superman.",port='5439')
# 
# finaldata <- dbGetQuery(redshift,"with t1 as (select channelid,sum(case when domain in ('1000bulbs.com') then .7 else .3 end * affinity_score) as affinity_score from bi_work.merchant_affinity_push where (domain= '1000bulbs.com' and affinity_score > 0)group by 1) select channelid from t1 where affinity_score >=0.73")


## takes dimensiontype, grabs all the necessary reactive input values for said dimensiontype, and combines into a dataframe
## returns dataframe
createQueryData <- function(dimensiontype,input) {
  
  
  inputIndeces <- which(grepl(dimensiontype,names(input)) & grepl("_",names(input)))
  inputNames <- names(input)[grepl(dimensiontype,names(input)) & grepl("_",names(input))]
  inputCategories <- unique(substr(names(input)[grepl(dimensiontype,names(input)) & grepl("_",names(input))], start = 0,
                                   stop = regexpr("_in",names(input)[grepl(dimensiontype,names(input)) & grepl("_",names(input))]) - 1))
  inputData <- data.frame(cbind(inputNames,
                                categories = substr(inputNames[grepl(dimensiontype,inputNames)], start = 0,
                                                    stop = regexpr("_in",inputNames[grepl(dimensiontype,inputNames)]) - 1)))
  
  querydata <- data.frame(t(aaply(inputCategories,1, function(z) {
    
    inputCategoryIndeces <- which(grepl(z,names(input)[grepl(dimensiontype,names(input)) & grepl("_",names(input))]) & 
                                    grepl("_",substr(names(input)[grepl(dimensiontype,names(input)) & grepl("_",names(input))], 
                                                     start = nchar(z)+ 1, stop = nchar(z) + 1)))
    
    aaply(as.character(inputData[inputData$categories == z,"inputNames"]), 1, function(x) {as.character(input[[x]])})
    
  })))
  
  colnames(querydata) <- inputCategories
  
  return(querydata)
}


##takes dataframe of one dimensiontype, and builds query
##need to modify so it takes multiple dimension types
##supports only affinity right
affinityQuery <- function(counter, dimensiontype, affinity.data, input) {
  
  
  n <- counter
  
  
  if (n == 0) {
    
    domaintext <- paste0("(", dimensiontype, "= '",
                         as.character(affinity.data[,dimensiontype])[1],
                         "' and affinity_score > 0)")
    
  }
  
  else {
    
    for (i in 0:(n)) {
      
      if (i == 0) {
        
        domaintext <- paste0("(", dimensiontype, "= '",
                             as.character(affinity.data[,dimensiontype])[i+1],
                             "' and affinity_score > 0) or ")
        
      }
      
      else if (i < (n)) {
        
        domaintext <- paste0(domaintext," (", dimensiontype, "= '",
                             as.character(affinity.data[,dimensiontype])[i+1],
                             "' and affinity_score > 0) or")
        
      }
      
      else {
        
        domaintext <- paste0(domaintext," (", dimensiontype, "= '",
                             as.character(affinity.data[,dimensiontype])[i+1],
                             "' and affinity_score > 0)")
        
      }
      
    }
    
  }
  
  query <- paste0("with t1 as (select channelid,platform, sum(case when domain in ('",
                  paste(as.character(affinity.data[as.character(affinity.data[,paste0(dimensiontype,"type")]) == "Primary",dimensiontype]), 
                        collapse = "','", sep = ""),
                  "') then .7 else .3 end * affinity_score) as affinity_score from bi_work.merchant_affinity_push where ",
                  domaintext, "group by 1,2) select channelid, platform from t1 where affinity_score >=", 
                  input[[paste0(dimensiontype,"threshold")]])
  
  return(query)
  
  
}


recipeBuilder <- function(counter, dimensiontype, affinity.data, input) {
  
  n <- counter
  
  recipes_firstlevel <-
    aaply(c(1:(n+1)), 1, function(x) {
      
      domain <- as.character(affinity.data[x,dimensiontype])
      paste0("name:recipe", x, ", ingredient:{source:merchant_affinity_push_v01, sourcetype:abacus_dataset,
             dimension:domain",", threshold:",domain, ", operator:", "=}, output:{channnelid, affinity_score}")
      
    })
  
  threshold <- input$domainthreshold
  
  ingredients <- 
    paste(
      aaply(c(1:(n+1)), 1, function(x) {
      
      weight <- ifelse(as.character(affinity.data[x,paste0(dimensiontype,"type")]) == "Primary", .7, .3)
      criteria <- 0
      
      paste0("ingredient:{source:recipe",x,", sourcetype:intersect_recipe}")
      
      }), collapse=", ", sep="")
  
  recipes_secondlevel <-
    paste0("name:push_affinity, ",ingredients, ", dimension:affinity_score", ", threshold:", threshold,
           ", operator:>=", ", output:{channelid}")
  
  return(t(c(recipes_firstlevel,recipes_secondlevel)))
  
}

splitSegments <- function(segments, campaignname) {
  
  query <- paste0("insert into bi_work.push_tool_audience 
                  with t1 as(
                    select channelid, platform, rank() over (partition by platform order by random()) as rank, count(*) over (partition by platform) as total
                    from bi_work.push_tool_affinity_stage where platform in ('Android-app','iOS-app'))
                  select channelid, case when platform = 'Android-app' then 'android' when platform = 'iOS-app' then 'ios' else 'other' end as platform
                  , ceiling(rank::numeric(18,10)/(total/",segments,")) as segment,'", 
                  campaignname, "' as campaign from t1;")
  
  return(query)
  
}
