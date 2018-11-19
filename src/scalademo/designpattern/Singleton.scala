package scalademo.designpattern

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/19
  * Time: 10:05 
  * Description:
  */
object Singleton {
    /*
        单例模式限制了一个类只能初始化一个对象，并且会提供一个全局引用指向它。
        在Java中，单例模式或许是最为被人熟知的一个模式了。这是java缺少某种语言特性的明显信号。
        在java中有static关键字，静态方法不能被任何对象访问，并且静态成员类不能实现任何借口。
        所以静态方法和Java提出的一切皆对象背离了。静态成员也只是个花哨的名字，本质上只不过是传统意义上的子程序。

        public class Cat implements Runnable {
            private static final Cat instance = new Cat();
            private Cat() {}
            public void run() {
                // do nothing
            }
            public static Cat getInstance() {
                return instance;
            }
        }
        Cat.getInstance().run()

        在Scala中完成单例简直巨简单无比。
        优势：
        ★ 含义明确
        ★ 语法简洁
        ★ 按需初始化
        ★ 线程安全
        劣势：
        ★ 对初始化行为缺乏控制
     */
    object Cat extends Runnable {
        def run() {
            // do nothing
        }
    }
    Cat.run()

}