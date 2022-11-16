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

        var rowSet = read(args[0]);

        //массивы для быстрого доступа по индексу
        ArrayList<String> validRows = new ArrayList(rowSet.size());
        ArrayList<String[]> vectorArr = new ArrayList(rowSet.size());

        //разбиваем строки на части
        //
        //выбираем валидные строки и определяем индексы для доступа к ним
        //
        //находим число столбцов

        int maxNumOfValues = 0;
        for(String row : rowSet){
            String[] vector = row.split(";");
            if(rowIsValid(vector)){ //здесь могла быть ваша валидация
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

        // Раскидываем подстроки по столбцам

        for (int i = 0; i < validRows.size(); i++) {
            String[] vector = vectorArr.get(i);
            for (int j = 0; j < vector.length; j++) {
                if (vector[j].length()>2) {
                    columns.get(j).add(new KV_Si_Struct(i, vector[j]));
                }
            }
        }

        //создаем список для подгрупп строк, которые должны быть объединены

        List<int[]> setsOfConnectedIndexes = new LinkedList();
        for (List<KV_Si_Struct> column : columns){
            //находим подгруппы в каждом столбце
            Map<String, List<KV_Si_Struct>> setsOfColumn = column.stream().
                    collect(Collectors.groupingBy(KV_Si_Struct::getString));
            //записываем подгруппы в список
            for (var elem : setsOfColumn.entrySet()){
                if(elem.getValue().size()>1){
                    int[] set = new int[elem.getValue().size()];
                    int i = 0;
                    for(KV_Si_Struct el: elem.getValue()){
                        set[i] = el.index;
                        i++;
                    }
                    setsOfConnectedIndexes.add(set);
                }
            }
        }

        //проходим по подгруппам и объединяем элементы в деревья (группы)

        QuickUnionPathHalvingUF uf = new QuickUnionPathHalvingUF(validRows.size());
        for(int[] setArr : setsOfConnectedIndexes){
            int papa = setArr[0];
            for (int i = 1; i < setArr.length; i++) {
                uf.union(papa, setArr[i]);
            }
        }

        //узнаем какой группе принадлежит каждая строка

        KV_ii_Struct[] membersOfGroup = new KV_ii_Struct[uf.parent.length];
        for (int i = 0; i < membersOfGroup.length; i++) {
            int papa = uf.find(uf.parent[i]);
            membersOfGroup[i] = new KV_ii_Struct(papa, i);
        }

        //группируем полученные key-value по ключу

        Map<Integer, List<KV_ii_Struct>> groupsMap = Arrays.stream(membersOfGroup)
                .collect(Collectors.groupingBy(KV_ii_Struct::getKey));

        //из полученных наборов формируем массивы индексов, входящих в одну группу

        ArrayList<int[]> listContainers = new ArrayList(groupsMap.keySet().size());
        var groupsIterator = groupsMap.values().iterator();
        for (int i = 0; i < groupsMap.keySet().size(); i++) { //проход по спискам индексов (группам)
            var group = groupsIterator.next();
            int[] itemsIndexes = new int[group.size()];
            var groupIterator = group.iterator();
            for (int j = 0; j < group.size(); j++) {
                itemsIndexes[j] = groupIterator.next().getValue();
            }
            listContainers.add(itemsIndexes);
        }

        // сортируем наборы по размеру

        Collections.sort(listContainers, (x, y) -> {
            return Integer.compare(x.length, y.length);
        });

        //находим кол-во групп, в которые входит более 1 элемента

        var bigGroups = listContainers.stream().filter(x->x.length>1).count();

        //выводим группы в файл по убыванию размера

        write("output.txt", listContainers, bigGroups, validRows);

        System.out.println("Уникальных строк: " + validRows.size());
        System.out.println("Групп: " + groupsMap.size());
        System.out.println("Больших групп: " + bigGroups);
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
        HashSet<String> set = new HashSet(1000000, 0.99f);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line != null) {
                set.add(line);
                line = reader.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return set;
    }

    public static void write(String path, ArrayList<int[]> groups, long bigGroupsCount, ArrayList<String> strings){
        try {
            Path newFilePath = Paths.get(path);
            Files.deleteIfExists(newFilePath);
            var x = Files.createFile(newFilePath);
            var sep = System.getProperty("line.separator");
            FileWriter writer = new FileWriter(x.toFile());
            int groupCounter = 0;
            writer.write("Групп с более чем одним элементом: " +  bigGroupsCount + sep + sep);
            for (int i = groups.size()-1; i >=0; i--) {
                writer.write("Группа " + ++groupCounter + sep + sep);
                var group = groups.get(i);
                for (int j = 0; j < group.length; j++) {
                    writer.write(strings.get(group[j]) + sep + sep);
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