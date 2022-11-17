import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TestAnalysis {
    private static final Pattern quotes = Pattern.compile("\"([\\s\\S]+?)\"|\"\"");

    public static void main(String[] args) {
        long m = System.currentTimeMillis();

        //получаем уникальные строки

        HashSet<String> rowSet = read(args[0]); //O(n)

        //массивы для быстрого доступа по индексу
        ArrayList<String> validRows = new ArrayList(rowSet.size());
        ArrayList<String[]> vectorArr = new ArrayList(rowSet.size());

        //разбиваем строки на части
        //
        //выбираем валидные строки и определяем индексы для доступа к ним
        //
        //находим число столбцов

        int maxNumOfValues = 0;
        for(String row : rowSet){ //O(N)
            String[] vector = row.split(";");//O(String.length)
            if(rowIsValid(vector)){ //O(vector.length)
                maxNumOfValues = Math.max(maxNumOfValues, vector.length);
                vectorArr.add(vector);
                validRows.add(row);
            }
        }

        // Создаем массив столбцов значений

        ArrayList<List<KV_Si_Struct>> columns = new ArrayList(maxNumOfValues);
        for (int i = 0; i < maxNumOfValues; i++) {
            columns.add(new ArrayList<KV_Si_Struct>());
        }

        // Распределяем подстроки по столбцам

        for (int i = 0; i < validRows.size(); i++) {
            String[] vector = vectorArr.get(i);
            for (int j = 0; j < vector.length; j++) {
                if (vector[j].length()>2) {
                    columns.get(j).add(new KV_Si_Struct(i, vector[j]));
                }
            }
        }

        //находим подгруппы, и объединяем элементы

        QuickUnionWithpathComressionUF uf = new QuickUnionWithpathComressionUF(validRows.size());
        for (List<KV_Si_Struct> column : columns){
            //находим подгруппы в каждом столбце
            Map<String, List<KV_Si_Struct>> setsOfColumn = column.stream().
                    collect(Collectors.groupingBy(KV_Si_Struct::getString));
            //записываем подгруппы в список
            for (var set: setsOfColumn.values()){
                if(set.size()>1){
                    int papa = set.get(0).getIndex();
                    for (int i = 1; i < set.size(); i++) {
                        uf.union(papa, set.get(i).getIndex());
                    }
                }
            }
        }

        //узнаем какой группе принадлежит каждая строка

        KV_ii_Struct[] membersOfGroup = new KV_ii_Struct[uf.parent.length];
        for (int i = 0; i < membersOfGroup.length; i++) {
            int papa = uf.find(uf.parent[i]);
            membersOfGroup[i] = new KV_ii_Struct(papa, i);
        }

        //компануем группы по ключу, сортируем по размеру и собираем в массив

        var groups = Arrays.stream(membersOfGroup)
                .collect(Collectors.groupingBy(KV_ii_Struct::getKey))
                .values().stream()
                .sorted(Comparator.comparingInt(x -> x.size()))
                .collect(Collectors.toCollection(ArrayList::new));

        //находим кол-во групп, в которые входит более 1 элемента

        long bigGroupsCount = 0;
        int groupIndex = groups.size()-1;
        while(groupIndex >= 0 && groups.get(groupIndex--).size()>1){
            bigGroupsCount++;
        }

        //выводим группы в файл по убыванию размера

        write("output.txt", groups, bigGroupsCount, validRows);

        System.out.println("Уникальных строк: " + validRows.size());
        System.out.println("Групп: " + groups.size());
        System.out.println("Больших групп: " + bigGroupsCount);
        System.out.println("Время выполнения: " + ((System.currentTimeMillis()-m)/1000 + 1) + " сек");
    }
    public static boolean rowIsValid(String[] parts){
        for (String s : parts){
            if(!quotes.matcher(s).matches()&&s!="")
                return false;
        }
        return true;
    }
    public static HashSet<String> read(String path){
        HashSet<String> rows = new HashSet();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                rows.add(line);
                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rows;
    }

    public static void write(String path, ArrayList<List<KV_ii_Struct>> groups, long bigGroupsCount, ArrayList<String> strings){
        try {
            Path newFilePath = Paths.get(path);
            Files.deleteIfExists(newFilePath);
            Path x = Files.createFile(newFilePath);
            String sep = System.getProperty("line.separator");
            FileWriter writer = new FileWriter(x.toFile());
            int groupCounter = 0;
            writer.write("Групп с более чем одним элементом: " +  bigGroupsCount + sep + sep);
            for (int i = groups.size()-1; i >=0; i--) {
                writer.write("Группа " + ++groupCounter + sep + sep);
                List<KV_ii_Struct> group = groups.get(i);
                for (var item : group) {
                    writer.write(strings.get(item.getValue()) + sep + sep);
                }
            }
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
//структура строка-индекс String-int для группировки по строке при помощи Stream api
class KV_Si_Struct {
    int index;
    String string;

    public KV_Si_Struct(int index, String string) {
        this.index = index;
        this.string = string;
    }

    public int getIndex() {
        return index;
    }

    public String getString() {
        return string;
    }
}
//структура ключ-значение int-int для группировки по ключу при помощи Stream api
class KV_ii_Struct {
    int key;
    int value;

    public KV_ii_Struct(int key, int value) {
        this.key = key;
        this.value = value;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}