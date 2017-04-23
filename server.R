library(shiny)
source("functions.R")



shinyServer(function(input, output) {
  
  merchcounter <- reactiveValues(n = 0)
  
  observeEvent(input$addmerchant, {merchcounter$n <- merchcounter$n + 1})
  observeEvent(input$removemerchant, {merchcounter$n <- merchcounter$n - 1})
  
  output$Merchant <- renderUI({
    
    selectizeInput('domain_in0','Select Merchant:',
                   choices = sort(as.character(domains[,1])),
                   multiple = FALSE)
    
  })
  
  output$MerchantType <- renderUI({
    
    selectizeInput('domaintype_in0','Type:',
                   choices = c("Primary","Secondary"),
                   multiple = FALSE)
    
  })
  
  
  output$MerchantAffinity <- renderUI({
    
    sliderInput("domainthreshold","Affinity Threshold:",min = 0, max = 1, value = 0, step = .01, animate = FALSE)
    
  })
  
  merchselectmenus <- reactive({
    
    n <- merchcounter$n
    
    if (n > 0) {
      lapply(seq_len(n), function(i) {
        selectizeInput(paste0("domain_in", i),
                  "", choices = sort(as.character(domains[,1])), multiple = FALSE)
      })
    }
  })
  
  merchtypemenus <- reactive({
    
    n <- merchcounter$n
    
    if (n > 0) {
      lapply(seq_len(n), function(i) {
        selectizeInput(paste0("domaintype_in", i),
                       "", choices = c("Primary","Secondary"), multiple = FALSE)
      })
    }
  })
  
  
  output$merchselect_ui <- renderUI({merchselectmenus()})
  output$merchtype_ui <- renderUI({merchtypemenus()})

  
  
  affinityData <- eventReactive(input$createsegment, {

    
    createQueryData("domain",input)
    
    
  })
  
  executeQuery <- eventReactive(input$createsegment, {
    
    affinity.data <<- affinityData()
    
    n <- merchcounter$n
    
    query <- paste0("insert into bi_work.push_tool_affinity_stage ",affinityQuery(n,"domain",affinity.data, input))
    
    for (con in dbListConnections(drv)) dbDisconnect(con)
    
    drv <- dbDriver("PostgreSQL")
    
    if (!exists("redshift") | dbDisconnect(redshift) == TRUE) {
      
      
    }
    
    
    dbSendQuery(redshift, "truncate table bi_work.push_tool_affinity_stage;")
    dbSendQuery(redshift,query)
    dbSendQuery(redshift,"truncate table bi_work.push_tool_audience")
    dbSendQuery(redshift,splitSegments(input$testsegments,input$campaignname))
    
  })  
  
  
  
  output$finishstatus <- renderText({
    executeQuery()
    "done!"
    
    
    
    })
  
  output$recipe <- renderTable({
    
    recipeBuilder(merchcounter$n, "domain",affinityData(), input)
    
  })
  
  output$downloaddata <- downloadHandler(
    
    filename = function() {paste0(input$campaignname,".zip")},
      content = function(file1) {

      for (con in dbListConnections(drv)) dbDisconnect(con)  
                      
      drv <- dbDriver("PostgreSQL")
      if (!exists("redshift") | dbDisconnect(redshift) == TRUE) {
        
        
        
      }
      
      filenames <-
        aaply(c(1:input$testsegments), 1, function(i) {
          
          write.csv(dbGetQuery(redshift,paste0("select * from bi_work.push_tool_audience where segment = ",i)), 
                    paste0(input$campaignname,i,".csv"))  
          paste0(input$campaignname,i,".csv")
          
        })
      
       zip(file1, filenames)
       print(file1)
       #browser()
      
      }, contentType = "application/zip"
        
    
  )
  
  
})
