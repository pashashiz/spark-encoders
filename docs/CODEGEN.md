# Codegen

Here is a code example to convert case class

```scala
case class UserOptName(name: Option[String], age: Int)

```

Into catalyst repr (unsafe row):

```java
import org.apache.spark.sql.catalyst.*;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.types.UTF8String;
import com.github.pashashiz.sparkenc.*;

class SpecificUnsafeProjection extends UnsafeProjection {

    private Object[] references;
    private boolean subExprIsNull_0;
    private boolean resultIsNull_0;
    // 
    private java.lang.String[] mutableStateArray_1 = new java.lang.String[1];
    private UnsafeRowWriter[] mutableStateArray_2 = new UnsafeRowWriter[1];
    private UserOptName[] mutableStateArray_0 = new UserOptName[1];

    public SpecificUnsafeProjection(Object[] references) {
        // references to external objects (i.e. error message)
        this.references = references;
        // UnsafeRow (catalyst repr memory)
        mutableStateArray_2[0] = new UnsafeRowWriter(2, 32);
    }

    public void initialize(int partitionIndex) {
    }

    // Scala.Function1 need this
    public java.lang.Object apply(java.lang.Object row) {
        return apply((InternalRow) row);
    }

    public UnsafeRow apply(InternalRow i) {
        // go to 0 position
        mutableStateArray_2[0].reset();
        // get input from sub expression
        subExpr_0(i);
        // clear all null bits
        mutableStateArray_2[0].zeroOutNullBytes();
        // property that indicates id name property is unwrapped as null
        resultIsNull_0 = false;
        // # Start Invoke 1.1 (obj.name.toString)
        if (!resultIsNull_0) {
            // #Start Invoke 1 (obj.name) (when returned object not nullable)
            // read name property as Option[String]
            boolean isNull_5 = true;
            // default value
            scala.Option value_5 = null;
            isNull_5 = false;
            if (!isNull_5) {
                // interesting!!! field value is not nullable, add assertNotNull?
                Object funcResult_0 = null;
                funcResult_0 = mutableStateArray_0[0].name();
                value_5 = (scala.Option) funcResult_0;
            }
            // # Start Unwrap option
            final boolean isNull_4 = isNull_5 || value_5.isEmpty();
            java.lang.String value_4 = isNull_4 ? null :
                    (java.lang.String) value_5.get();
            // # End Upwrap Option
            resultIsNull_0 = isNull_4;
            mutableStateArray_1[0] = value_4;
            // #End Invoke 1
        }
        // UTF8 to String is not null
        boolean isNull_3 = resultIsNull_0;
        UTF8String value_3 = null;
        if (!resultIsNull_0) {
            value_3 = UTF8String.fromString(mutableStateArray_1[0]);
        }
        // # End Invoke 1.1 (obj.name.toString)
        if (isNull_3) {
            // if null set field as nullable
            mutableStateArray_2[0].setNullAt(0);
        } else {
            // if not null write value
            mutableStateArray_2[0].write(0, value_3);
        }

        // #Start Invoke 2 (obj.age) (when returned primitive type and no try/catch)
        boolean isNull_6 = true;
        // default value
        int value_6 = -1;
        isNull_6 = false;
        if (!isNull_6) {
            // read age property as Int, hm what if that is not primitive?
            value_6 = mutableStateArray_0[0].age();
        }
        // #End Invoke 2
        mutableStateArray_2[0].write(1, value_6);
        // build unsafe row
        return (mutableStateArray_2[0].getRow());
    }

    private void subExpr_0(InternalRow i) {
        UserOptName value_2 = (UserOptName) i.get(0, null);
        if (false) {
            throw new NullPointerException(((java.lang.String) references[0] /* errMsg */));
        }
        subExprIsNull_0 = false;
        mutableStateArray_0[0] = value_2;
    }
}
```

From catalyst repr:

```java
import com.github.pashashiz.sparkenc.UserOptAge;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BaseProjection;
import org.apache.spark.unsafe.types.UTF8String;

class SpecificSafeProjection extends BaseProjection {

    private Object[] references;
    private InternalRow mutableRow;
    private java.lang.String[] mutableStateArray_0 = new java.lang.String[1];
    private scala.Option[] mutableStateArray_1 = new scala.Option[1];

    public SpecificSafeProjection(Object[] references) {
        this.references = references;
        mutableRow = (InternalRow) references[references.length - 1];
    }

    public void initialize(int partitionIndex) {}

    public java.lang.Object apply(java.lang.Object _i) {
        InternalRow i = (InternalRow) _i;
        UTF8String value_2 = i.getUTF8String(0);
        boolean isNull_1 = true;
        java.lang.String value_1 = null;
        isNull_1 = false;
        if (!isNull_1) {
            Object funcResult_0 = null;
            funcResult_0 = value_2.toString();
            value_1 = (String) funcResult_0;
        }
        mutableStateArray_0[0] = value_1;

        boolean isNull_4 = i.isNullAt(1);
        int value_4 = isNull_4 ? -1 : (i.getInt(1));
        scala.Option value_3 = isNull_4 ? scala.Option$.MODULE$.apply(null) : new scala.Some(value_4);
        mutableStateArray_1[0] = value_3;
        
        final UserOptAge value_0 = false ? null : new UserOptAge(mutableStateArray_0[0], mutableStateArray_1[0]);
        if (false) {
            mutableRow.setNullAt(0);
        } else {
            mutableRow.update(0, value_0);
        }
        return mutableRow;
    }
}
```