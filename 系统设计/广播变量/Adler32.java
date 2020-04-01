/**
 * Adler32算法
 * @author xyf
 * 20200401
 * 算法原理:
 * Adler32校验算法通过计算两个16位A和B的校验数,将其连在一起构成校验和
 * A是流中所有字节的数量加1,B是A每一步A的和
 * 初始化情况下,A=1,B=0
 * {{{
 *  A = 1 + D1 + D2 + ... + Dn (mod 65521)

    B = (1 + D1) + (1 + D1 + D2) + ... + (1 + D1 + D2 + ... + Dn) (mod 65521)
    = n×D1 + (n−1)×D2 + (n−2)×D3 + ... + Dn + n (mod 65521)

    Adler-32(D) = B × 65536 + A
 * }}}
 * 
 * 
 */
public class Aler32 {

    static int a=1;
    static int b=0;
    // 设置最大递归栈深度，防止栈内存溢出
    static int MAX_DEPTH=1000;

    static void caculate(String s,int depth){
        if(depth==s.length() || depth>MAX_DEPTH)
            return;
        a+=Integer.valueOf(s.charAt(depth));
        b+=a;
        caculate(s, depth+1);
    }

    static void encode(String s){
        caculate(s, 0);
        int res=(b % 65521)*65536+(a%65521);
        System.out.println(Integer.toHexString(res));
    }

    public static void main(String[] args) {
        String s="Wikipedia";
        encode(s);
    }
}