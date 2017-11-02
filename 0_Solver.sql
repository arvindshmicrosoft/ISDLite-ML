USE ISDLite
GO

exec sp_execute_external_script @language = N'R',
@script = N'
# Here are the equations corresponding to the original puzzle:
# a + b = 8
# c - d = 6
# a + c = 13
# b + d = 8

# paste these into a matrix depicting coefficients for each variable
# a+b+c+ d = res
# 1 1 0  0 = 8
# 0 0 1 -1 = 6
# 1 0 1  0 = 13
# 0 1 0  1 = 8

# code the left hand side matrix
M1 = matrix(data=c(1,1,0,0,0,0,1,-1,1,0,1,0,0,1,0,1), nrow=4, ncol=4, byrow=TRUE)
M2 = matrix(data=c(8,6,13,8), nrow=4, ncol=1, byrow=TRUE)

OutputDataSet <- as.data.frame(solve(M1,M2))
'