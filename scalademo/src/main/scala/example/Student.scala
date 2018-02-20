package example

class Student(id:Int, name:String){   
    var age:Int = 0   
    def showDetails(){   
        println(id+" "+name+" "+age)   
    }   
    def this(id:Int, name:String,age:Int){   
        this(id,name)       
        this.age = age   
    }   
}   