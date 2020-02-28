require(xts)

str_path = '/path/to/daily/datasets/'
stocks = c('MSFT','AAPL','AMZN','GOOG','BRKB','JPM','JNJ',
          'XOM','V','BAC','INTC','PG','CSCO','DIS','HD','VZ',
          'CVX','MA')

merge_sigs <- function(price){
  oh <- ifelse(price$open > lag(price$high), log(price$open) - log(lag(price$high)), 0)
  ol <- ifelse(price$open < lag(price$low), log(price$open) - log(lag(price$low)), 0)
  pnls <- log(price$close) - log(price$open)
  px_up <- cbind(oh, pnls)
  px_up <- px_up[px_up[,1] != 0]
  px_dn <- cbind(ol, pnls)
  px_dn <- px_dn[px_dn[,1] != 0]
  data <- rbind(px_up, px_dn)
  colnames(data) <- c('x','y')
  data <- scale(data)
  return(data)
}
read_data <- function(stocks){
  df <- data.frame()
  for(stock in stocks){
    price <- read.csv(paste0(str_path, stock, '.csv'), stringsAsFactors = FALSE)
    price$ticker <- price$dividends <- price$closeunadj <- price$lastupdated <- NULL
    price <- as.xts(price[2:6], as.Date(price$date))
    data <- merge_sigs(price)
    
    if(length(df) == 0){
      df <- data
    } else {
      df <- rbind(df, data)
    }
  }
  return(df)
}
plot_data <- function(df, scale=1){
  df_p <- df[df$x > 0]
  df_n <- df[df$x < 0]
  plot(as.vector(df$x), as.vector(df$y), type='n', ylim = c(-scale,scale),
       xlab='opening gap', ylab='day move')
  abline(h=0, lty=3)
  #abline(lm(as.vector(df$x)~as.vector(df$y)), col="blue", lty=3)
  #lines(lowess(as.vector(df$x),as.vector(df$y)), col="blue", lty=3)
  lines(lowess(as.vector(df_p$x),as.vector(df_p$y)), col="green")
  lines(lowess(as.vector(df_n$x),as.vector(df_n$y)), col="red")
}
plot_hist <- function(df, x=0, lt=TRUE, hist=TRUE, breaks=100){
  if(lt){
    y <- df$y[df$x < x]
  } else{
    y <- df$y[df$x > x]
  }
  if(hist){
    hist(as.vector(y),breaks = breaks)
  } else{
    plot(density(y), xlab='move', ylab='probability', main='move')
  }
  print(quantile(y))
}


df <- read_data(stocks)

df <- df['2016::2017']
plot_data(df, 2)
hist(as.vector(df$x), breaks = 100)
hist(as.vector(df$y), breaks = 100)
plot_hist(df,x=-1,lt=TRUE,hist=TRUE)

mid <- df[abs(df$x)<2]
plot_data(mid, 0.5)
plot_hist(mid,x=-0.75,lt=TRUE,hist=FALSE)
