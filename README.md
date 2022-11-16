# Алгоритм обработки строковых массивов
## Описание
### Что делает алгоритм?
Алгоритм извлекает строки, содержащие наборы значений, из входного файла и группирует их по следующему критерию:  
ЕСЛИ в строках А и Б имеются значения с индесом i, и эти значения равны, ТО строки попадают в одну группу  
( if a[i]==b[i] then union(a, b) )  
Притом если А и Б объединены в группу, и Б и В объединены в группу, то А и В должны оказаться в одной группе.  
То есть приведенный критерий является транзитивным.  
Также производится валидация строк: если одно из значений строки не пустое, и при этом не является "" или "\"\"" , то строка считается невалидной и далее не обрабатывается.  
[Задание на разработку](https://github.com/PeacockTeam/new-job/blob/master/lng%26java).  
### Как работает алгоритм?
 __Образный пример:__ представьте себе двор, в котором очень много детей, например миллион) Каждый из детей имеет разноцветные элементы одежды. А теперь представьте, что вам нужно разделить этих детей на группы таким образом, чтобы у любых детей из любых двух групп не нашлось бы элемента одежды одинакового цвета. Например внутри группы может быть такое, что у двух детей есть одинаковые розовые варежки, или одинаковые куртки.
 Можно конечно сравнить одежду на всех детях, но это будет слишком долго. Можно поступить хитрее: назначить каждому ребенку номер и попросить каждого ребенка написать его на всей своей одежде. После этого можно разложить одежду в кучки (кучка курток, кучка варежек) и рассортировать по цвету. После этого можно записать в блокнот наборы номеров одинаковых элементов. Ура, теперь вы знаете, какие дети точно оказались бы в одной группе! Осталось только обработать записи в блокноте...)  
   
 __Реализация:__ Сперва производится выборка уникальных строк, их валидация и подсчет максимального числа значений в строке. 
Затем все значения заносятся в списки, соответствующие их индексу в строке. Эти списки группируются по значению и из групп формируются наборы индексов строк, которые должны оказаться в одной группе. После этого, на основе этих наборов, строится дерево Union-Find, в котором множество строк разбиваются на непересекающиеся множества. Далее массив извлекается из структуры и группируется по значению (значение = номер группы). И, наконец, когда имеются сгрупированные индексы, строки группами выводятся в файл после предварительной сортировки по размеру.  
Также алгоритм замеряет время выполнения, количество уникальных строк, общее количество групп, и количество групп, в которые входит более одной строки.  
### Сторонние классы и библиотеки  
__1. Структура Union-Find:__ представляет собой небинарное дерево элементов, основанное на целочисленном массиве.  
[Подробнее посмотреть принцип работы дерева можно здесь](https://www.youtube.com/watch?v=ayW5B2W9hfo).  
Код структуры взят [отсюда](https://algs4.cs.princeton.edu/15uf/).  
## Результаты тестирования на наборах данных
Тестирование производилось на двух файлах:  
- Первый содержит 1млн строк переменной длинны (от 1 до 11 значений в строке)  
- Второй содержит 12млн строк постоянной длинны (3 значения в строке)  
Результаты тестирования приведены ниже.  
Запуск производился с __ограничением памяти 1 Гб__ (-Xmx1G)
#### Результат тестирования на первом файле [lng.txt](https://github.com/PeacockTeam/new-job/releases/download/v1.0/lng-4.txt.gz):  
![image](https://user-images.githubusercontent.com/57357300/201470663-1d224963-8130-46cc-a97c-6ed0620ae768.png)
#### Результат тестирования на втором файле [lng-big.csv](https://github.com/PeacockTeam/new-job/releases/download/v1.0/lng-big.7z):  
![image](https://i.ibb.co/Q8TBqjB/image.png)
