package scalademo.practice

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/20
  * Time: 9:00 
  * Description:
  */
object Demo02 {
    //  2、使用Scala实现二分查找算法。给定一个排序的数据[2,6,9,12,32,43,56,61]，给定要查找的数字，
    // 如果查找到则返回对应数字的下标，如果未查询到结果则返回下标-1
    /*思路：
    假如有一组数为3，12，24，36，55，68，75，88要查给定的值24.可设三个变量front，mid，end分别指向数据的上界，中间和下界，mid=（front+end）/2.
      1.开始令front=0（指向3），end=7（指向88），则mid=3（指向36）。因为a[mid]>x，故应在前半段中查找。
      2.令新的end=mid-1=2，而front=0不变，则新的mid=1。此时x>a[mid]，故确定应在后半段中查找。
      3.令新的front=mid+1=2，而end=2不变，则新的mid=2，此时a[mid]=x，查找成功。
      如果要查找的数不是数列中的数，例如x=25，当第三次判断时，x>a[mid]，按以上规律，令front=mid+1，即front=3，出现front>end的情况，表示查找不成功。
     */

    /**
      * 普通算法：二分法查找数据的下标
      * @param arr 要遍历的数组
      * @param value 要查询的数据
      * @return 返回查询到的下标，没有查询到，返回-1
      */
    def binary(arr: Array[Int], value: Int): Int = {
        var front = 0
        var end = arr.length - 1
        var flag = true //没有break和continue，只能flag了。
        var mid = 0
        while (front <= end && flag) {
            mid = (front + end) / 2
            if (value == arr(mid)) {
                flag = false
            }
            if (value > arr(mid)) {
                front = mid + 1
            }
            if (value < arr(mid)) {
                end = mid - 1
            }
        }
        if (front > end) -1 else mid
    }
    //递归算法
    def recurbinary(arr: Array[Int], value: Int,front:Int,end:Int): Int ={
        if(front>end) return -1 //return还是能用的
        val mid = (front+end)/2
        if(arr(mid) == value){
            mid
        }else if(arr(mid) > value){
            recurbinary(arr,value,front,mid-1)
        }else{
            recurbinary(arr,value,mid+1,end)
        }
    }

    println(binary(Array(2, 6, 9, 12, 32, 43, 56, 61), 12))
    println(recurbinary(Array(2, 6, 9, 12, 32, 43, 56, 61), 12,0,7))
}