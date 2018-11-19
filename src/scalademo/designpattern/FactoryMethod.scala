package scalademo.designpattern

/**
  * Created with IntelliJ IDEA.
  * User: lihaoran 
  * Date: 2018/11/19
  * Time: 9:55 
  * Description:
  */
object FactoryMethod {

    /*
        工厂方法模式将对实际类的初始化封装在一个方法中，让子类来决定初始化哪个类。
        工厂方法允许：
        1、组合复杂的对象创建代码
        2、选择需要初始化的类
        3、缓存对象
        4、协调对共享资源的访问
        我们考虑静态工厂模式，这和经典的工厂模式略有不同，静态工厂方法避免了子类来覆盖此方法。

        在Java中，我们使用new关键字，通过调用类的构造器来初始化对象。
        为了实现这个模式，我们需要依靠普通方法，此外我们无法在接口中定义静态方法，所以我们只能使用一个额外的工厂类。
        public interface Animal {}
        private class Dog implements Animal {}
        private class Cat implements Animal {}
        public class AnimalFactory {
            public static Animal createAnimal(String kind) {
                if ("cat".equals(kind)) return new Cat();
                if ("dog".equals(kind)) return new Dog();
                throw new IllegalArgumentException();
            }
        }
        AnimalFactory.createAnimal("dog");

        除了构造器之外，Scala提供了一种类似于构造器调用的特殊的语法，其实这就是一种简便的工 厂模式

     */

    trait Animal

    private class Dog extends Animal

    private class Cat extends Animal

    object Animal {
        def apply(kind: String) = kind match {
            case "dog" => new Dog()
            case "cat" => new Cat()
        }
    }

    Animal("dog")

    /*
    以上代码中，工厂方法被定义为伴生对象，它是一种特殊的单例对象，和之前定义的类或特质具有相同的名字，并且需要定义在同一个原文件中。
    这种语法仅限于工厂模式中的静态工厂模式，因为我们不能将创建对象的动作代理给子类来完成。
    优势：
    ★ 重用基类名字
    ★ 标准并且简洁
    ★ 类似于构造器调用
    劣势：
    ★ 仅限于静态工厂方法
     */

}