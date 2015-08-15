/*

expand 2th col with value of 4th col

*/

val x = sc.parallelize(List( ("a" , 1 ,  3, 1) , ("c" , 2, 4, 1) , ("c" , 3 , 6, 1) , ("a" , 4, 9, 1)))
val a =x.map(t=>(t._1,  (t._2,  t._4)))
val colname = x.map(_._2).distinct.collect().sortWith(_<_).zipWithIndex.toMap

//1--------
val b = a.groupByKey()
val c = b.mapValues(t=> {
    val arr = new Array[Int](colname.size)    
    t.map(m=> 
        { 
            val index = colname(m._1)
            arr(index) = m._2 + arr(index)
        })
    arr
})


//2------
val c = a.combineByKey( 
    value => {
        val arr  = new Array[Int](colname.size)
        arr(colname(value._1)) = value._2
        arr
    },
    (x:Array[Int],  value) => {
        x(colname(value._1)) = value._2
        x
    }, 
    (x: Array[Int], y: Array[Int]) => {        
        for( i <- 0 until x.size)  x(i)=x(i)+y(i)
        x
    }
)
