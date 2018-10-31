package scalademo

import utils.BaseUtil.int2boolen

object OrderDemo extends App {
    /*
    （1）sorted
    对一个集合进行自然排序，通过传递隐式的Ordering
    （2）sortBy
    对一个属性或多个属性进行排序，通过它的类型。
    （3）sortWith
    基于函数的排序，通过一个comparator函数，实现自定义排序的逻辑。
     */

    val 基于单集合单字段的排序 = 0
    if (0) {
        val xs = Seq(1, 5, 3, 4, 6, 2)
        println("==============sorted排序=================")
        println(xs.sorted) //升序
        println(xs.sorted.reverse) //降序
        println("==============sortBy排序=================")
        println(xs.sortBy(d => d)) //升序
        println(xs.sortBy(d => d).reverse) //降序
        println("==============sortWith排序=================")
        println(xs.sortWith(_ < _)) //升序
        println(xs.sortWith(_ > _)) //降序
    }

    val 基于元组多字段的排序 = 0
    if (0) {
        //注意多字段的排序，使用sorted比较麻烦，这里给出使用sortBy和sortWith的例子

        //先看基于sortBy的实现：
        val pairs = Array(("a", 5, 1), ("c", 3, 1), ("b", 1, 3))
        //按第三个字段升序，第一个字段降序，注意，排序的字段必须和后面的tuple对应
        val bx = pairs.sortBy(r => (r._3, r._1))(Ordering.Tuple2(Ordering.Int, Ordering.String.reverse))


        //再看基于sortWith的实现：
        val pairs2 = Array(("a", 5, 1), ("c", 3, 1), ("b", 1, 3))
        val b = pairs2.sortWith {
            case (a, b) => {
                if (a._3 == b._3) {
                    //如果第三个字段相等，就按第一个字段降序
                    a._1 > b._1
                } else {
                    a._3 < b._3 //否则第三个字段降序
                }
            }
        }
        //从上面可以看出，基于sortBy的第二种实现比较优雅，语义比较清晰，第三种灵活性更强，但代码稍加繁琐

    }

    val 基于类的排序 = 0
    if(0){
        //先看sortBy的实现方法 排序规则：先按年龄排序，如果一样，就按照名称降序排
        case class Person(val name:String,val age:Int)

        val p1=Person("cat",23)
        val p2=Person("dog",23)
        val p3=Person("andy",25)

        val pairs = Array(p1,p2,p3)

        //先按年龄排序，如果一样，就按照名称降序排
        val bx= pairs.sortBy(person => (person.age, person.name))( Ordering.Tuple2(Ordering.Int, Ordering.String.reverse) )
        bx.map(println)

    }

}
