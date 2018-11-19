package scalademo.designpattern

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/19
  * Time: 10:09 
  * Description:
  */
object Decorator {
    /*
    装饰模式被用来在不影响一个类其它实例的基础上扩展一些对象的功能。装饰者是对继承的一个灵活替代。
    当需要有很多独立的方式来扩展功能时，装饰者模式是很有用的，这些扩展可以随意组合。
    在Java中，需要新建一个装饰类，实现原来的接口，封装原来实现接口的类，不同的装饰者可以组合起来使用。
    一个处于中间层的装饰者一般会用来代理原接口中很多的方法。

    public interface OutputStream {
        void write(byte b);
        void write(byte[] b);
    }
    public class FileOutputStream implements OutputStream { /* ... */ }
    public abstract class OutputStreamDecorator implements OutputStream {
        protected final OutputStream delegate;
        protected OutputStreamDecorator(OutputStream delegate) {
            this.delegate = delegate;
        }
        public void write(byte b) { delegate.write(b); }
        public void write(byte[] b) { delegate.write(b); }
    }
    public class BufferedOutputStream extends OutputStreamDecorator {
        public BufferedOutputStream(OutputStream delegate) {
            super(delegate);
        }
        public void write(byte b) {
            // ...
            delegate.write(buffer)
        }
    }
    new BufferedOutputStream(new FileOutputStream("foo.txt"))


    Scala提供了一种更直接的方式来重写接口中的方法，并且不用绑定到具体实现。下面看下 如何来使用abstract override标识符。
     */

    /*
    trait OutputStream {
        def write(b: Byte)
        def write(b: Array[Byte])
    }
    class FileOutputStream(path: String) extends OutputStream { /* ... */ }
    trait Buffering extends OutputStream {
        abstract override def write(b: Byte) {
            // ...
            super.write(buffer)
        }
    }
    new FileOutputStream("foo.txt") with Buffering // with Filtering, ...

    */

}