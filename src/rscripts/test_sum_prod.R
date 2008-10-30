#base <- 2.0
#values <- c(1.0, 2.0, 3.0, 4.0, 5.0)

# Returns sum(values) + base
sumfx <- function(base, values) {
    result <- base
	for (i in 1:length(values)){
		result <- result + values[i]
	}
    result
}

# Returns product(values) + base
prodfx <- function(base, values) {
    result <- values[1]
	for (i in 2:length(values)){
		result <- result * values[i]
	}
    result + base
}

# Perform the calculations based on the inputs. Store the values
# in the outputs
sum <- sumfx(base, values)
prod <- prodfx(base, values)
comb <- c(sum,prod)
