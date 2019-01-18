package algo

import utils.BaseUtil._

import scala.util.Random
import scala.util.control.Breaks.{break, breakable}


/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2019/1/18
  * Time: 14:29 
  * Description:
  */
object SortDemo extends App {

  //简单数组
  val arr1 = {
    Array(4, 5, 6, 1, 2, 3)
  }

  //生成一个巨大的随机数组，用来排序
  val arr2 = {
    val n = 30000 //数组大小
    val arr = Array.fill(n)(Random.nextInt(n)) //随机生成0到n-1中的任一个
    println(s"原数组：${arr.mkString(",")}")
    arr
  }

  val 冒泡排序 = 0
  if (0) {
    def bubbleSort(items: Array[Int]): Array[Int] = {
      val length = items.length
      breakable {
        for (i <- Range(0, length)) {
          var exit = true
          for (j <- Range(0, length - i - 1)) {
            if (items(j + 1) < items(j)) {
              val temp = items(j + 1)
              items(j + 1) = items(j)
              items(j) = temp
              exit = false
            }
          }
          if (exit) {
            break
          }
        }
      }
      items
    }

    val (res, time) = getMethodRunTime(bubbleSort(arr2))
    println(res.mkString(","))
    println(time)


  }

  val 插入排序demodfasdasdf = 0
  if (0) {
    def insertSort(items: Array[Int]): Array[Int] = {
      val length = items.length
      for (i <- Range(1, length)) {
        val value = items(i)
        var j = i - 1
        breakable {
          while (j >= 0) {
            if (items(j) > value) {
              items(j + 1) = items(j)
            } else {
              break
            }
            j -= 1
          }
        }
        items(j + 1) = value
      }
      items
    }
    val (res, time) = getMethodRunTime(insertSort(arr2))
    println(res.mkString(","))
    println(time)
  }

  val 选择排序 = 0
  if(0){
    def selectionSort(items: Array[Int]): Array[Int] = {
      val length = items.length
      for (i <- Range(0, length)) {
        var minIndex = i
        for (j <- Range(i + 1, length)) {
          if (items(j) < items(minIndex)) {
            minIndex = j
          }
        }

        //put the min value to the front
        val temp = items(i)
        items(i) = items(minIndex)
        items(minIndex) = temp
      }
      items
    }
    val (res, time) = getMethodRunTime(selectionSort(arr2))
    println(res.mkString(","))
    println(time)
  }


}