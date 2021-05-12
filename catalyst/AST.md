#### 设计模式 访问者模式

访问者模式是一种**行为型**设计模式, 提供基于某对象结构的各种操作方式, 可以在不改变对象结构的前提下. 定义作用于元素的新操作. 如果数据结构比较稳定, 但是函数易变, 那么使用访问者模式是比较适宜的. 如果数据结构易变, 则不适宜使用.



访问者模式一共有5个角色.

1. `Vistor`抽象访问者,为该对象结构中具体角色申明一个访问接口
2. `ConcreteVisitor`具体访问者, 实现抽象访问者中定义的操作
3. `Element` 定义一个`accept`接受操作,以`vistor`作为参数
4. `ConcreteElement`具体元素, 实现了`accept`逻辑
5. `ObjectStructure`对象结构, 可以是组合模式,也可以是集合,能够枚举所包含的元素.提供一个接口,允许`vistor`访问.

**示例:**

如果老师教学反馈得分大于等于85分、学生成绩大于等于90分，则可以入选成绩优秀奖；如果老师论文数目大于8、学生论文数目大于2，则可以入选科研优秀奖。



1. 定义访问者

   ```java
   public interface Visitor {
   
       void visit(Student element);
   
       void visit(Teacher element);
   
   }
   ```

2. 定义具体访问者

   ```java
   public class GradeSelectionVistor implements Visitor{
       private String awardWords = "[%s]的分数是%d，荣获了成绩优秀奖。";
   
       @Override
       public void visit(Student element) {
           // 如果学生考试成绩超过90，则入围成绩优秀奖。
           if (element.getGrade() >= 90) {
               System.out.println(String.format(awardWords, 
                       element.getName(), element.getGrade()));
           }
       }
   
       @Override
       public void visit(Teacher element) {
           // 如果老师反馈得分超过85，则入围成绩优秀奖。
           if (element.getScore() >= 85) {
               System.out.println(String.format(awardWords, 
                       element.getName(), element.getScore()));
           }
       }
   }		
   ```

   ```java
   public class ResearcherSelectionVistor implements Visitor {
       private String awardWords = "[%s]的论文数是%d，荣获了科研优秀奖。";
   
       @Override
       public void visit(Student element) {
           // 如果学生发表论文数超过2，则入围科研优秀奖。
           if(element.getPaperCount() > 2){
               System.out.println(String.format(awardWords,
                       element.getName(),element.getPaperCount()));
           }
       }
   
       @Override
       public void visit(Teacher element) {
           // 如果老师发表论文数超过8，则入围科研优秀奖。
           if(element.getPaperCount() > 8){
               System.out.println(String.format(awardWords,
                       element.getName(),element.getPaperCount()));
           }
       }
   }
   ```

3. 定义抽象元素

   ```java
   public interface Element {
   
       //接受一个抽象访问者访问
       void accept(Visitor visitor);
   
   }
   ```

4. 定义具体元素

   ```java
   public class Teacher implements Element {
   
       private String name; // 教师姓名
       private int score; // 评价分数
       private int paperCount; // 论文数
   
       // 构造器
       public Teacher(String name, int score, int paperCount) {
           this.name = name;
           this.score = score;
           this.paperCount = paperCount;
       }
   
       // visitor访问本对象的数据结构
       @Override
       public void accept(Visitor visitor) {
           visitor.visit(this);
       }
   
       public String getName() {
           return name;
       }
   
       public void setName(String name) {
           this.name = name;
       }
   
       public int getScore() {
           return score;
       }
   
       public void setScore(int score) {
           this.score = score;
       }
   
       public int getPaperCount() {
           return paperCount;
       }
   
       public void setPaperCount(int paperCount) {
           this.paperCount = paperCount;
       }
   }
   ```

   ```java
   public class Student implements Element {
   
       private String name; // 学生姓名
       private int grade; // 成绩
       private int paperCount; // 论文数
   
       // 构造器
       public Student(String name, int grade, int paperCount) {
           this.name = name;
           this.grade = grade;
           this.paperCount = paperCount;
       }
   
       // visitor访问本对象的数据结构
       @Override
       public void accept(Visitor visitor) {
           visitor.visit(this);
       }
   
       public String getName() {
           return name;
       }
   
       public void setName(String name) {
           this.name = name;
       }
   
       public int getGrade() {
           return grade;
       }
   
       public void setGrade(int grade) {
           this.grade = grade;
       }
   
       public int getPaperCount() {
           return paperCount;
       }
   
       public void setPaperCount(int paperCount) {
           this.paperCount = paperCount;
       }
   }
   ```

5. 定义对象结构

   ```java
   public class ObjectStructure {
   
       // 使用集合保存Element元素，示例没有考虑多线程的问题。
       private ArrayList<Element> elements = new ArrayList<>();
   
       /**
        * 访问者访问元素的入口
        *
        * @param visitor 访问者
        */
       public void accept(Visitor visitor) {
           for (int i = 0; i < elements.size(); i++) {
               Element element = elements.get(i);
               element.accept(visitor);
           }
       }
   
       /**
        * 把元素加入到集合
        *
        * @param element 待添加的元素
        */
       public void addElement(Element element) {
           elements.add(element);
       }
   
       /**
        * 把元素从集合中移除
        *
        * @param element 要移除的元素
        */
       public void removeElement(Element element) {
           elements.remove(element);
       }
   }
   ```

6. 简单使用逻辑

   ```java
   public class VisitorClient {
   
       public static void main(String[] args) {
           // 初始化元素
           Element stu1 = new Student("Student Jim", 92, 3);
           Element stu2 = new Student("Student Ana", 89, 1);
           Element t1 = new Teacher("Teacher Mike", 83, 10);
           Element t2 = new Teacher("Teacher Lee", 88, 7);
           // 初始化对象结构
           ObjectStructure objectStructure = new ObjectStructure();
           objectStructure.addElement(stu1);
           objectStructure.addElement(stu2);
           objectStructure.addElement(t1);
           objectStructure.addElement(t2);
           // 定义具体访问者，选拔成绩优秀者
           Visitor gradeSelection = new GradeSelection();
           // 具体的访问操作，打印输出访问结果
           objectStructure.accept(gradeSelection);
           System.out.println("----结构不变，操作易变----");
           // 数据结构是没有变化的，如果我们还想增加选拔科研优秀者的操作，那么如下。
           Visitor researcherSelection = new ResearcherSelection();
           objectStructure.accept(researcherSelection);
       }
   }
   ```



#### 设计模式 观察者模式

也是一种**行为型**设计模式. 当一个对象被修改的时候, 自动通知其他对象. 可以作消息总线.

观察者模式包含如下四个角色:

1. 观察目标 `subject`:  提供被观察者的通用接口
2. 具体目标  `ConcreteSubject`: 目标类的子类, 通常包含发生改变的数据, 当状态发生改变的时候, 会向各个观察者通知.同时实现了目标类的抽象方法.
3. 观察者 `observer`: 观察目标的改变,进而做出相应, 通常为接口.  该接口声明了更新数据的方法`update`.也叫做抽象观察者.
4. 具体观察者 `ConcreteObserver`: 维护一个指向具体对象的引用. 实现了抽象观察者`update`方法,实现更新逻辑. 通常会通过`attach`方法将自己添加到目标类通知列表中.或者使用`detach`从列表中移除.

**示例:**

1. 定义观察者接口

   ```java
   interface Observer {
       public void update();
   }
   ```

2. 定义观察目标

   ```java
   abstract class Subject {
       private Vector<Observer> obs = new Vector();
   
       public void addObserver(Observer obs){
           this.obs.add(obs);
       }
       public void delObserver(Observer obs){
           this.obs.remove(obs);
       }
       protected void notifyObserver(){
           for(Observer o: obs){
               o.update();
           }
       }
       public abstract void doSomething();
   }
   ```

3. 定义具体观察者

   ```java
   class ConcreteSubject extends Subject {
       public void doSomething(){
           System.out.println("被观察者事件发生改变");
           this.notifyObserver();
       }
   }
   ```

4. 定义具体的被观察者

   ```java
   class ConcreteObserver1 implements Observer {
       public void update() {
           System.out.println("观察者1收到信息，并进行处理");
       }
   }
   class ConcreteObserver2 implements Observer {
       public void update() {
           System.out.println("观察者2收到信息，并进行处理");
       }
   }
   ```

5. 简单的客户端逻辑

   ```java
   public class Client {
       public static void main(String[] args){
           Subject sub = new ConcreteSubject();
           sub.addObserver(new ConcreteObserver1()); //添加观察者1
           sub.addObserver(new ConcreteObserver2()); //添加观察者2
           sub.doSomething();
       }
   }
   ```



#### AST介绍



