./a4.1.out 0

i=0
while [[ $i -lt 12 ]]
do
	echo "running test: " $i
	./a4.1.out $i
	i=$(( $i + 1 ))
done
