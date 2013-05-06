# This file contains simple examples of logistic regression


# Define the logistic function
logit <- function(x) 1/(1+exp(-x))

# Compute the gradient
grad <- function(w, x, y, lambda = 0) {
  (-t(x) %*% (y * (1.0 - logit(y*(x %*% w)))) + lambda * w) / nrow(x)
}

# Compute the loglikelihood of the w for the data
llik <- function(w, x, y, lambda = 0) {
  sum(log(logit( y * (x %*% w) ))) + lambda * t(w) %*% w
}

propCorrect <- function(w, x, y) mean( y * (x %*% w)  > 0 )



# Classic batch gradient descent
batchGD <- function(niter, x, y, 
                    w0 = matrix(rnorm(ncol(x)), ncol(x), 1), lambda = 0) {
  w <- w0
  N <- nrow(x)
  D <- ncol(x)
  llikArray <- array(0, niter + 1)
  touchedData <- array(0, niter + 1)
  llikArray[1] <- llik(w,x,y,lambda)
  for(i in 1:niter) {
    # I am using an decreasing learning rate
    w <- w - (1/i) * grad(w, x, y, lambda)
    #w <- w - grad(w, x, y, lambda)
    
    llikArray[i+1] <- llik(w, x, y, lambda)
    touchedData[i+1] <- N
  }
  list(w=w, llik=llikArray, touchedData=cumsum(touchedData)) 
}

# Classic batch gradient descent
miniBatchGD <- function(niter, x, y, nblocks,
           w0 = matrix(rnorm(ncol(x)), ncol(x), 1), lambda = 0) {
  w <- w0
  N <- nrow(x)
  D <- ncol(x)
  blockSize <- ceiling(N / nblocks) 
  nsteps <- niter*nblocks
  llikArray <- array(0, nsteps + 1)
  touchedData <- array(0, nsteps + 1)
  llikArray[1] <- llik(w,x,y,lambda)
  for(i in 1:nsteps) {
    ind <- sample(N, blockSize)
    # I am using an decreasing learning rate
    w <- w - (1/i) * grad(w, x[ind,], y[ind,], lambda)
    #w <- w - grad(w, x[ind,], y[ind,], lambda)
    llikArray[i+1] <- llik(w, x, y, lambda)
    touchedData[i+1] <- blockSize
  }
  list(w=w, llik=llikArray, touchedData=cumsum(touchedData)) 
}

SGD <- function(niter, x, y, nblocks,
                w0 = matrix(rnorm(ncol(x)), ncol(x), 1), lambda = 0) {
  # I used blocks in SGD to keep track of when to recompute the "objective" 
  # so the objective is not computed too frequently (otherwise this would be 
  # painfully slow)
  
  w <- w0
  N <- nrow(x)
  D <- ncol(x)
  blockSize <- ceiling(N / nblocks) 
  nsteps <- niter*nblocks
  llikArray <- array(0, nsteps + 1)
  touchedData <- array(0, nsteps + 1)
  llikArray[1] <- llik(w,x,y,lambda)
  c <- 1
  for(i in 1:nsteps) {
    for(k in 1:blockSize) {
      ind <- sample(N, 1)
      # I am using an decreasing learning rate
      w <- w - (1/c) * grad(w, matrix(x[ind,],1,D), y[ind], lambda)
      #w <- w - grad(w, matrix(x[ind,],1,D), y[ind], lambda)
      c <- c + 1
    }
    llikArray[i+1] <- llik(w, x, y, lambda)
    touchedData[i+1] <- blockSize
  }
  list(w=w, llik=llikArray, touchedData=cumsum(touchedData)) 
}

miniBatchSGD <- function(niter, x, y, nblocks,
                w0 = matrix(rnorm(ncol(x)), ncol(x), 1), lambda = 0) {
  w <- w0
  N <- nrow(x)
  D <- ncol(x)
  blockSize <- ceiling(N / nblocks) 
  nsteps <- niter
  llikArray <- array(0, nsteps + 1)
  touchedData <- array(0, nsteps + 1)
  llikArray[1] <- llik(w,x,y,lambda)
  c <- 1
  for(i in 1:nsteps) {
    wOld <- w
    cOld <- c
    wSum <- matrix(0,D,1)
    # run nblocks of SGD in "isolation"
    for(b in 1:nblocks) {
      w <- wOld
      c <- cOld
      for(k in 1:blockSize) {
        ind <- sample(N, 1)
        # I am using an decreasing learning rate
        w <- w - (1/c) * grad(w, matrix(x[ind,],1,D), y[ind], lambda)
        #w <- w - grad(w, matrix(x[ind,],1,D), y[ind], lambda)
        c <- c + 1
      }
      wSum <- wSum + w
    }
    # Average the new weights
    w <- wSum / nblocks
    
    c <- cOld + blockSize
    llikArray[i+1] <- llik(w, x, y, lambda)
    touchedData[i+1] <- nblocks * blockSize
  }
  list(w=w, llik=llikArray, touchedData=cumsum(touchedData)) 
}


# Make data
D <- 100
N <- 100000

wTrue <- matrix(rnorm(D), D,1)
x <- matrix(rnorm(D*N), N, D)
z <- x %*% wTrue 
prob <- logit(z)
y <- matrix(sapply(runif(N) < prob, function(x) if(x) 1 else -1), N, 1)

w0 = matrix(rnorm(D), D, 1)
niter <- 20
gradRes <- batchGD(niter, x, y, w0)
nblocks <- 10
mbgradRes <- miniBatchGD(niter, x, y, nblocks, w0)
sgdRes <- SGD(niter, x, y, nblocks, w0)
mbsgdRes <- miniBatchSGD(niter, x, y, nblocks, w0)




yrange <- range(c(gradRes$llik, mbgradRes$llik, 
                  sgdRes$llik, mbsgdRes$llik))
xrange <- range(c(gradRes$touchedData, mbgradRes$touchedData, 
                  sgdRes$touchedData, mbsgdRes$touchedData))
plot(gradRes$touchedData, gradRes$llik, xlim = xrange, ylim = yrange,
     xlab = "Points Touched", ylab = "Loglikelihood")
lines(mbgradRes$touchedData, mbgradRes$llik, type="o", col=2)
lines(sgdRes$touchedData, sgdRes$llik, type="o", col=3)
lines(mbsgdRes$touchedData, mbsgdRes$llik, type="o", col=4)


legend("bottomright", c("SGD", "MBSGD", "MBGrad", "Grad"), pch=1,
       col=c(3,4,2,1))

  


