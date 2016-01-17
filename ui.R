library(shiny)


shinyUI(fluidPage(
  
  titlePanel("Push A/B Testing"),
  
  tabsetPanel(
    tabPanel("Segment Creation",
             
             hr(),
             fluidRow(
               column(3,
                      textInput("campaignname","Campaign Name:")
                      ),
               column(3,
                      selectizeInput("testsegments","Segments",choices = c(1:5))
                      )
               
             ),
             hr(),
             fluidRow(
               
               column(3,
                      uiOutput("Merchant"),
                      uiOutput("merchselect_ui"),
                      actionButton("addmerchant","Add Merchant"),
                      actionButton("removemerchant","Remove Merchant")),
               column(3,
                      uiOutput("MerchantType"),
                      uiOutput("merchtype_ui")),
               column(3,
                      uiOutput("MerchantAffinity"))
               
             ),
             hr(),
             fluidRow(
               actionButton("createsegment","Create Segment"),
               textOutput("finishstatus"),
               hr(),
               downloadButton('downloaddata','Download'),
               hr(),
               tableOutput("recipe")
             ))
    
  )
  
  
  
))