0. For Pairs implementation, there consists of three MapReduce jobs. The first job maps raw input data into word pairs, the intermediate  key-value pair is <PairOfStrings, FloatWritable>. With hash table control, exactly same word pair will only be emitted once from the mapper per line. I have hard code <(A,"\1"), value> in order to record the total number of a single word. The output key-value pair is<PairOfStrings, FloatWritable> where the key is (x,y) and the corresponding value is p(x,y)/(p(x)^2). Then second job takes the first job output as input, upon recieving <(x,y) value>, mapper emits <(x,y) value> and <(y,x) value>. No combiner is needed in this job as for each key, there will be only 2 instances. The reducer emitting value will equals the nature log of lines read multiply square root of the multiplication of the two values of a corresponding key. In the third job, secondary sort is used to sort the output first with the left word in the pair and then with PMI.
   
    For Stripes implementation, the last two MapReduce job are the same. But the first job maps raw input data into a word map.Before mapping, I have deleted all repeated words in a line with two scanning over a line. The intermediate  key-value pair is <Text, String2IntOpenHashMapWritable>. The marginal of every word is put under entry "\1" of the stripe of the corresponding word. The output key-value pair is still <PairOfStrings, FloatWritable> where the key is (x,y) and the corresponding value is p(x,y)/(p(x)^2).

1.  Pairs: 291.497s    Stripe: 160.887s

2.  Pairs: 334.291s    Stripe: 160.159s

3.  233518

4.  (abednego, meshch), (meshch, shadrach), (abednego,shadrach)   9.319931  It is because they appear less, i.e. only 14 times. But they all come together 

5.  cloud:     tabernacle glory fire
    
    love:      hate hermia commandments 
