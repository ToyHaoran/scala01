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
    val n = 40000 //数组大小
    val arr = Array.fill(n)(Random.nextInt(n)) //随机生成0到n-1中的任一个
    println(s"原数组：${arr.mkString(",")}")
    arr
  }

  val ___________n2___________ = 0

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

  val 插入排序 = 0
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
  if (0) {
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

  val ___________nlogn___________ = 0

  val 归并排序 = 0
  if (0) {
    def mergeSort(items: Array[Int]): Array[Int] = {
      _mergeSort(items, 0, items.length - 1)
      items
    }
    def _mergeSort(items: Array[Int], p: Int, r: Int): Unit = {
      if (p >= r) {
        return
      }
      val q = p + (r - p) / 2
      _mergeSort(items, p, q)
      _mergeSort(items, q + 1, r)
      _merge(items, p, q, r)
    }
    def _merge(items: Array[Int], p: Int, q: Int, r: Int): Unit = {
      //start of first half
      var i = p
      //start of second half
      var j = q + 1
      var k = 0
      //temp array to hold the data
      val tempArray = new Array[Int](r - p + 1)
      while (i <= q && j <= r) {
        if (items(i) <= items(j)) {
          tempArray(k) = items(i)
          i += 1
        } else {
          tempArray(k) = items(j)
          j += 1
        }
        k += 1
      }
      var start = i
      var end = q
      if (j <= r) {
        start = j
        end = r
      }
      for (n <- start to end) {
        tempArray(k) = items(n)
        k += 1
      }
      //copy tempArray back to items
      for (n <- 0 to r - p) {
        items(p + n) = tempArray(n)
      }
    }

    val (res, time) = getMethodRunTime(mergeSort(arr2))
    println(res.mkString(","))
    println(time)
  }

  val 快速排序 = 0
  if (1) {
    //find the K th smallest element int the array
    def findKthElement(items: Array[Int], k: Int): Int = {
      _findKthElement(items, k, 0, items.length - 1)
    }

    def _findKthElement(items: Array[Int], k: Int, p: Int, r: Int): Int = {
      val q = _partition(items, p, r)

      if (k == q + 1) {
        items(q)
      } else if (k < q + 1) {
        _findKthElement(items, k, p, q - 1)
      } else {
        _findKthElement(items, k, q + 1, r)
      }
    }

    def quickSort(items: Array[Int]): Array[Int] = {
      _quickSort(items, 0, items.length - 1)
      items
    }

    def _quickSort(items: Array[Int], p: Int, r: Int): Unit = {
      if (p >= r) {
        return
      }
      val q = _partition(items, p, r)
      _quickSort(items, p, q - 1)
      _quickSort(items, q + 1, r)
    }

    def _partition(items: Array[Int], p: Int, r: Int): Int = {
      val pivot = items(r)
      var i = p
      for (j <- Range(p, r)) {
        if (items(j) < pivot) {
          val temp = items(i)
          items(i) = items(j)
          items(j) = temp
          i += 1
        }
      }

      val temp = items(i)
      items(i) = items(r)
      items(r) = temp

      i
    }

    val (res, time) = getMethodRunTime(quickSort(arr2))
    println(res.mkString(","))
    println(time)
  }


}